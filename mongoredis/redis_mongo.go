package mongoredis

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/daqiancode/scache"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var MongoOpTimeout = 600 * time.Second

type RedisMongo[T scache.Table[I], I string] struct {
	*scache.CacheBase[T, I]
	db         *mongo.Client
	red        *scache.RedisJson[T]
	redId      *scache.RedisJson[string]
	redIds     *scache.RedisJson[[]string]
	database   string
	collection string
	c          *mongo.Collection
}

func NewRedisMongo[T scache.Table[I], I string](prefix, database, table, idField string, db *mongo.Client, red *redis.Client, ttl time.Duration) *RedisMongo[T, I] {
	return &RedisMongo[T, I]{
		CacheBase:  scache.NewCacheBase[T, I](prefix, table, idField),
		db:         db,
		red:        scache.NewRedisJson[T](red, ttl),
		redId:      scache.NewRedisJson[string](red, ttl),
		redIds:     scache.NewRedisJson[[]string](red, ttl),
		database:   database,
		collection: table,
		c:          db.Database(database).Collection(table),
	}
}

func (s *RedisMongo[T, I]) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), MongoOpTimeout)
	defer cancel()
	return s.db.Disconnect(ctx)
}
func (s *RedisMongo[T, I]) ClearCache(id I, indexes scache.Indexes) error {

	var keys []string
	if !scache.IsNullID(id) {
		keys = append(keys, s.MakeCacheKey(scache.NewIndex(s.GetIdField(), id)))
	}
	for _, v := range indexes {
		keys = append(keys, s.MakeCacheKey(v))
	}
	keys = scache.UniqueStrings(keys)
	ctx, cancel := context.WithTimeout(context.Background(), MongoOpTimeout)
	defer cancel()
	return s.red.Del(ctx, keys...).Err()
}

func (s *RedisMongo[T, I]) Get(id I) (T, bool, error) {
	var t T
	ctx, cancel := context.WithTimeout(context.Background(), MongoOpTimeout)
	defer cancel()
	r := s.c.FindOne(ctx, bson.M{"_id": id})
	if err := r.Err(); err != nil {
		if mongo.ErrNoDocuments == err {
			return t, false, nil
		}
		return t, false, err
	}
	err := r.Decode(&t)
	return t, true, err
}

func (s *RedisMongo[T, I]) List(ids ...I) ([]T, error) {
	var t []T
	var err error
	// objectIds := make([]primitive.ObjectID, len(ids))

	// for i, v := range ids {
	// 	objectIds[i], err = primitive.ObjectIDFromHex(scache.Stringify(v, ""))
	// 	if err != nil {
	// 		return t, err
	// 	}
	// }
	ctx, cancel := context.WithTimeout(context.Background(), MongoOpTimeout)
	defer cancel()
	query := bson.M{"_id": bson.M{"$in": ids}}
	r, err := s.c.Find(ctx, query)
	if err != nil {
		return t, err
	}
	// err = r.Decode(&t)
	err = r.All(ctx, &t)
	return t, err
}

func (s *RedisMongo[T, I]) Create(t *T) error {
	if t == nil {
		return nil
	}
	if scache.IsNullID((*t).GetID()) {
		reflect.ValueOf(t).Elem().FieldByName(s.GetIdField()).SetString(primitive.NewObjectID().Hex())
	}
	ctx, cancel := context.WithTimeout(context.Background(), MongoOpTimeout)
	defer cancel()
	_, err := s.c.InsertOne(ctx, *t)
	if err != nil {
		return err
	}
	s.ClearCache((*t).GetID(), (*t).ListIndexes())
	return nil
}
func (s *RedisMongo[T, I]) Save(t *T) error {
	if t == nil {
		return nil
	}
	id := (*t).GetID()
	if scache.IsNullID(id) {
		return s.Create(t)
	}
	_, exist, err := s.Get(id)
	if err != nil {
		return nil
	}
	if !exist {
		return s.Create(t)
	}
	query := bson.M{"_id": id}
	ctx, cancel := context.WithTimeout(context.Background(), MongoOpTimeout)
	defer cancel()
	err = s.c.FindOneAndReplace(ctx, query, *t).Err()
	return err
}

func (s *RedisMongo[T, I]) Delete(ids ...I) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	objs, err := s.List(ids...)
	if err != nil {
		return 0, err
	}
	// objectIds := make([]primitive.ObjectID, len(ids))

	// for i, v := range ids {
	// 	objectIds[i], err = primitive.ObjectIDFromHex(scache.Stringify(v, ""))
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	ctx, cancel := context.WithTimeout(context.Background(), MongoOpTimeout)
	defer cancel()
	query := bson.M{"_id": bson.M{"$in": ids}}
	rs, err := s.c.DeleteMany(ctx, query)
	if err != nil {
		return 0, err
	}
	for _, v := range objs {
		s.ClearCache(v.GetID(), v.ListIndexes())
	}
	return rs.DeletedCount, err
}

// Update values type: map[string]interface{} , eg:map[string]interface{}{"addr.country": "uae", "tags.0.name": "gg"}
func (s *RedisMongo[T, I]) Update(id I, values interface{}) (int64, error) {
	if scache.IsNullID(id) {
		return 0, nil
	}
	// objectId, err := primitive.ObjectIDFromHex(scache.Stringify(id, ""))
	// if err != nil {
	// 	return err
	// }
	old, _, err := s.Get(id)
	if err != nil {
		return 0, err
	}
	var setD bson.D
	if m, ok := values.(map[string]interface{}); ok {
		for k, v := range m {
			setD = append(setD, bson.E{Key: k, Value: v})
		}
	} else {
		return 0, errors.New("RedisMongo.Update not support this type of update values, only support map[string]interface{}")
	}
	setValues := bson.D{{Key: "$set", Value: setD}}
	ctx, cancel := context.WithTimeout(context.Background(), MongoOpTimeout)
	defer cancel()
	rs, err := s.c.UpdateOne(ctx, bson.M{"_id": id}, setValues)

	if err != nil {
		return 0, err
	}
	newObj, _, err := s.Get(id)
	if err != nil {
		return 0, err
	}
	err = s.ClearCache(old.GetID(), old.ListIndexes().Merge(newObj.ListIndexes()))

	return rs.MatchedCount, nil
}

func (s *RedisMongo[T, I]) GetBy(index scache.Index) (T, bool, error) {
	var t T
	ctx, cancel := context.WithTimeout(context.Background(), MongoOpTimeout)
	defer cancel()
	r := s.c.FindOne(ctx, index)
	if err := r.Err(); err != nil {
		if mongo.ErrNoDocuments == err {
			return t, false, nil
		}
		return t, false, err
	}
	err := r.Decode(&t)
	return t, true, err
}

func (s *RedisMongo[T, I]) ListBy(index scache.Index, initOrders scache.OrderBys) ([]T, error) {
	var t []T
	var err error
	// objectIds := make([]primitive.ObjectID, len(ids))

	// for i, v := range ids {
	// 	objectIds[i], err = primitive.ObjectIDFromHex(scache.Stringify(v, ""))
	// 	if err != nil {
	// 		return t, err
	// 	}
	// }
	var opts *options.FindOptions
	if len(initOrders) > 0 {
		ds := make([]bson.E, len(initOrders))
		for i, v := range initOrders {
			ds[i] = bson.E{Key: v.Field, Value: v.Asc}
		}
		opts = options.Find().SetSort(ds)
	}
	ctx, cancel := context.WithTimeout(context.Background(), MongoOpTimeout)
	defer cancel()
	r, err := s.c.Find(ctx, index, opts)
	if err != nil {
		return t, err
	}
	// err = r.Decode(&t)
	err = r.All(ctx, &t)
	return t, err
}
