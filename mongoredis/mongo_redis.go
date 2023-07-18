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

func NewMongoRedis[T scache.Table[I], I scache.IDType](prefix, database, collection, idField string, db *mongo.Client, red *redis.Client, ttl time.Duration) *scache.RedisCache[T, I] {
	m := &Mongo[T, I]{
		db:         db,
		idField:    idField,
		ctx:        context.Background(),
		database:   database,
		collection: collection,
		c:          db.Database(database).Collection(collection),
	}
	rc := scache.NewRedisCache[T, I](prefix, collection, idField, m, red, ttl)
	return rc
}

func NewMongoRedisFull[T scache.Table[I], I scache.IDType](prefix, database, collection, idField string, db *mongo.Client, red *redis.Client, ttl time.Duration) *scache.FullRedisCache[T, I] {
	m := &Mongo[T, I]{
		db:         db,
		idField:    idField,
		ctx:        context.Background(),
		database:   database,
		collection: collection,
		c:          db.Database(database).Collection(collection),
	}
	rc := scache.NewFullRedisCache[T, I](prefix, collection, idField, m, red, ttl)
	return rc
}

type Mongo[T scache.Table[I], I scache.IDType] struct {
	db         *mongo.Client
	idField    string
	ctx        context.Context
	database   string
	collection string
	c          *mongo.Collection
}

func (s *Mongo[T, I]) Close() error {
	return s.db.Disconnect(s.ctx)
}

func (s *Mongo[T, I]) DB() *mongo.Client {
	return s.db
}

func (s *Mongo[T, I]) Create(t *T) error {
	if scache.IsNullID((*t).GetID()) {
		reflect.ValueOf(t).Elem().FieldByName(s.idField).SetString(primitive.NewObjectID().Hex())
	}
	_, err := s.c.InsertOne(s.ctx, *t)
	return err
}

func (s *Mongo[T, I]) Save(t *T) error {
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
	err = s.c.FindOneAndReplace(s.ctx, query, *t).Err()
	return err
}

func (s *Mongo[T, I]) Update(id I, values interface{}) (int64, error) {
	if scache.IsNullID(id) {
		return 0, nil
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
	rs, err := s.c.UpdateOne(s.ctx, bson.M{"_id": id}, setValues)

	if err != nil {
		return 0, err
	}
	return rs.MatchedCount, nil
}
func (s *Mongo[T, I]) Delete(ids ...I) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	// _, err := s.List(ids...)
	// if err != nil {
	// 	return 0, err
	// }
	// objectIds := make([]primitive.ObjectID, len(ids))

	// for i, v := range ids {
	// 	objectIds[i], err = primitive.ObjectIDFromHex(scache.Stringify(v, ""))
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	query := bson.M{"_id": bson.M{"$in": ids}}
	rs, err := s.c.DeleteMany(s.ctx, query)
	if err != nil {
		return 0, err
	}

	return rs.DeletedCount, err
}
func (s *Mongo[T, I]) Get(id I) (T, bool, error) {
	var t T
	r := s.c.FindOne(s.ctx, bson.M{"_id": id})
	if err := r.Err(); err != nil {
		if mongo.ErrNoDocuments == err {
			return t, false, nil
		}
		return t, false, err
	}
	err := r.Decode(&t)
	return t, true, err
}
func (s *Mongo[T, I]) GetBy(index scache.Index) (T, bool, error) {
	var t T
	r := s.c.FindOne(s.ctx, index)
	if err := r.Err(); err != nil {
		if mongo.ErrNoDocuments == err {
			return t, false, nil
		}
		return t, false, err
	}
	err := r.Decode(&t)
	return t, true, err
}
func (s *Mongo[T, I]) List(ids ...I) ([]T, error) {
	var t []T
	var err error
	query := bson.M{"_id": bson.M{"$in": ids}}
	r, err := s.c.Find(s.ctx, query)
	if err != nil {
		return t, err
	}
	// err = r.Decode(&t)
	err = r.All(s.ctx, &t)
	return t, err
}
func (s *Mongo[T, I]) ListBy(index scache.Index, orderBys scache.OrderBys) ([]T, error) {
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
	if len(orderBys) > 0 {
		ds := make([]bson.E, len(orderBys))
		for i, v := range orderBys {
			ds[i] = bson.E{Key: v.Field, Value: v.Asc}
		}
		opts = options.Find().SetSort(ds)
	}

	r, err := s.c.Find(s.ctx, index, opts)
	if err != nil {
		return t, err
	}
	// err = r.Decode(&t)
	err = r.All(s.ctx, &t)
	return t, err
}
func (s *Mongo[T, I]) ListAll() ([]T, error) {
	var t []T
	r, err := s.c.Find(s.ctx, bson.D{})
	if err != nil {
		return t, err
	}
	err = r.All(s.ctx, &t)
	return t, err
}

func (s *Mongo[T, I]) ListByUniqueInts(field string, values []int64) ([]T, error) {
	var t []T
	var err error
	query := bson.M{field: bson.M{"$in": values}}
	r, err := s.c.Find(s.ctx, query)
	if err != nil {
		return t, err
	}
	// err = r.Decode(&t)
	err = r.All(s.ctx, &t)
	return t, err
}

func (s *Mongo[T, I]) ListByUniqueStrs(field string, values []string) ([]T, error) {
	var t []T
	var err error
	query := bson.M{field: bson.M{"$in": values}}
	r, err := s.c.Find(s.ctx, query)
	if err != nil {
		return t, err
	}
	// err = r.Decode(&t)
	err = r.All(s.ctx, &t)
	return t, err
}
