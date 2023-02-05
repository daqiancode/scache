package scache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache[T Table[I], I IDType] struct {
	*CacheBase[T, I]
	red    *RedisJson[T]
	redId  *RedisJson[I]
	redIds *RedisJson[[]I]
	db     DBCRUD[T, I]
}

func NewRedisCache[T Table[I], I IDType](prefix, table, idField string, db DBCRUD[T, I], red *redis.Client, ttl time.Duration) *RedisCache[T, I] {
	return &RedisCache[T, I]{
		CacheBase: &CacheBase[T, I]{prefix: prefix, table: table, idField: idField},
		red:       NewRedisJson[T](red, ttl),
		redId:     NewRedisJson[I](red, ttl),
		redIds:    NewRedisJson[[]I](red, ttl),
		db:        db,
	}
}

// func (s *RedisCache[T, I]) SetDB(db DBCRUD[T, I]) {
// 	s.db = db
// }

func (s *RedisCache[T, I]) GetDB() DBCRUD[T, I] {
	return s.db
}
func (s *RedisCache[T, I]) Close() error {
	return s.db.Close()
}
func (s *RedisCache[T, I]) ClearCache(objs ...T) error {
	if len(objs) == 0 {
		return nil
	}
	var keys []string
	for _, v := range objs {
		keys = append(keys, s.MakeCacheKey(NewIndex(s.GetIdField(), v.GetID())))
		for _, u := range v.ListIndexes() {
			keys = append(keys, s.MakeCacheKey(u))
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	keys = UniqueStrings(keys)
	return s.red.Del(ctx, keys...).Err()

}

// func (s *RedisCache[T, I]) ClearCacheRaw(id I, indexes Indexes) error {
// 	var keys []string
// 	if !IsNullID(id) {
// 		keys = append(keys, s.MakeCacheKey(NewIndex(s.GetIdField(), id)))
// 	}
// 	for _, v := range indexes {
// 		keys = append(keys, s.MakeCacheKey(v))
// 	}
// 	keys = UniqueStrings(keys)
// 	return s.red.Del(s.ctx, keys...).Err()
// }

func (s *RedisCache[T, I]) Create(obj *T) error {
	if err := s.db.Create(obj); err != nil {
		return err
	}
	s.ClearCache(*obj)
	// s.ClearCache((*obj).GetID(), (*obj).ListIndexes())
	return nil
}
func (s *RedisCache[T, I]) Delete(ids ...I) (int64, error) {
	objs, err := s.List(ids...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := s.db.Delete(ids...)
	if err != nil {
		return 0, err
	}
	s.ClearCache(objs...)
	// for _, v := range objs {
	// 	err = s.ClearCache(v.GetID(), v.ListIndexes())
	// }
	return rowsAffected, err
}
func (s *RedisCache[T, I]) Save(obj *T) error {
	old, exists, err := s.Get((*obj).GetID())
	if err != nil {
		return err
	}
	if IsNullID((*obj).GetID()) || !exists {
		if err := s.db.Create(obj); err != nil {
			return err
		}
	} else {
		if err := s.db.Save(obj); err != nil {
			return err
		}
	}
	s.ClearCache(old, *obj)
	return nil
}

// Update values can be struct or map[string]interface{}
func (s *RedisCache[T, I]) Update(id I, values interface{}) (int64, error) {
	if IsNullID(id) {
		return 0, nil
	}
	old, _, err := s.Get(id)
	if err != nil {
		return 0, err
	}
	effectedRows, err := s.db.Update(id, values)
	if err != nil {
		return 0, err
	}

	obj, _, err := s.Get(id)
	s.ClearCache(old, obj)
	// err = s.ClearCache(old.GetID(), old.ListIndexes().Merge(obj.ListIndexes()))
	return effectedRows, err
}

func (s *RedisCache[T, I]) Get(id I) (T, bool, error) {
	redisKey := s.MakeCacheKey(NewIndex(s.GetIdField(), id))
	r, exists, err := s.red.GetJson(redisKey)
	if err != nil {
		return r, false, err
	}
	if exists {
		s.red.Expires(redisKey)
		return r, true, nil
	}
	r, exists, err = s.db.Get(id)
	if err != nil {
		return r, false, err
	}
	if !exists {
		err = s.red.SetNull(redisKey)
		return r, exists, err
	}
	err = s.red.SetJson(redisKey, r)
	return r, true, err
}

// List list records by ids, order & empty records keeped
func (s *RedisCache[T, I]) List(ids ...I) ([]T, error) {
	// fetch records from redis by ids
	redisKeys := make([]string, len(ids))
	for i, v := range ids {
		redisKeys[i] = s.MakeCacheKey(NewIndex(s.GetIdField(), v))
	}
	cachedRecords, missedIndexes, err := s.red.MGetJson(redisKeys)
	if err != nil {
		return nil, err
	}
	if len(missedIndexes) == 0 {
		s.red.Expires(redisKeys...)
		return cachedRecords, err
	}
	cachedIdIndexMap := make(map[I]bool, len(cachedRecords))

	for _, v := range cachedRecords {
		cachedIdIndexMap[v.GetID()] = true
	}
	// 没有命中的Id(key)
	missedIds := make([]I, len(missedIndexes))
	//没有命中的id索引
	missedIdIndexMap := make(map[I]int)
	for i, v := range missedIndexes {
		missedIds[i] = ids[v]
		missedIdIndexMap[ids[v]] = i
	}

	// for i, v := range ids {
	// 	if !cachedIdIndexMap[v] {
	// 		missedIds = append(missedIds, v)
	// 		missedIdIndexMap[v] = i
	// 	}
	// }
	// if len(missedIds) == 0 {
	// 	return cachedRecords, nil
	// }
	// search missed record from database
	var missedRecords []T
	missedRecords, err = s.db.List(missedIds...)
	if err != nil {
		return cachedRecords, err
	}
	needToCache := make(map[string]interface{}, len(missedRecords))
	needToCacheNull := make([]string, len(missedIds)-len(missedRecords))

	//数据库中存在的id
	dbIds := make(map[I]bool)
	for _, v := range missedRecords {
		needToCache[s.MakeCacheKey(NewIndex(s.GetIdField(), v.GetID()))] = v
		cachedRecords[missedIdIndexMap[v.GetID()]] = v
		dbIds[v.GetID()] = true
	}
	//数据库中不存在的objs
	i := 0
	for _, v := range missedIds {
		if !dbIds[v] {
			needToCacheNull[i] = s.MakeCacheKey(NewIndex(s.GetIdField(), v))
			i++
		}
	}
	s.red.MSetJson(needToCache)
	s.red.MSetNull(needToCacheNull)
	return cachedRecords, nil

}
func (s *RedisCache[T, I]) GetBy(index Index) (T, bool, error) {
	// fetch id from redis
	redisKey := s.MakeCacheKey(index)
	var r T
	cachedId, exists, err := s.redId.GetJson(redisKey)
	if err != nil && err != redis.Nil {
		return r, false, err
	}
	if exists && IsNullID(cachedId) {
		return r, false, nil
	}
	if exists {
		s.red.Expires(redisKey)
		return s.Get(cachedId)
	}
	// search from db
	r, exists, err = s.db.GetBy(index)
	if err != nil {
		return r, false, err
	}
	if !exists {
		err = s.red.SetNull(redisKey)
		return r, exists, err
	}
	// set id to redis
	err = s.redId.SetJson(redisKey, r.GetID())
	return r, true, err
}
func (s *RedisCache[T, I]) ListBy(index Index, orderBys OrderBys) ([]T, error) {
	// fetch ids from redis
	redisKey := s.MakeCacheKey(index)
	var r []T
	cachedIds, exists, err := s.redIds.GetJson(redisKey)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if exists {
		s.red.Expires(redisKey)
		return s.List(cachedIds...)
	}
	// search from db
	r, err = s.db.ListBy(index, orderBys)
	if err != nil {
		return nil, err
	}
	ids := make([]I, len(r))
	for i, v := range r {
		ids[i] = v.GetID()
	}
	// set ids to redis
	err = s.redIds.SetJson(redisKey, ids)
	return r, err
}
