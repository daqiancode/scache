package scache

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type FullDBCache[T Table[I], I IDType] interface {
	DBCRUD[T, I]
	// Create(r *T) error
	// Save(r *T) error
	// Update(id I, values interface{}) (int64, error)
	// Delete(ids ...I) (int64, error)
	// Get(id I) (T, bool, error)
	// List(ids ...I) ([]T, error)
	ListAll() ([]T, error)
	// Close() error
}

type FullRedisCache[T Table[I], I IDType] struct {
	*CacheBase[T, I]
	db     FullDBCache[T, I]
	red    *RedisHashJson[T, I]
	ctx    context.Context
	redId  *RedisJson[I]
	redIds *RedisJson[[]I]
}

func NewFullRedisCache[T Table[I], I IDType](prefix, table, idField string, db FullDBCache[T, I], red *redis.Client, ttl time.Duration) *FullRedisCache[T, I] {
	return &FullRedisCache[T, I]{
		CacheBase: &CacheBase[T, I]{prefix: prefix, table: table, idField: idField},
		db:        db,
		red:       NewRedisHashJson[T, I](red, ttl),
		ctx:       context.Background(),
		redId:     NewRedisJson[I](red, ttl),
		redIds:    NewRedisJson[[]I](red, ttl),
	}
}

func (s *FullRedisCache[T, I]) CacheKey() string {
	r := s.prefix + "/" + s.table + "/full"
	return strings.ToLower(r)
}

func (s *FullRedisCache[T, I]) Load() error {
	r, err := s.db.ListAll()
	if err != nil {
		return err
	}

	key := s.CacheKey()
	err = s.red.HSetJson(key, r...)
	if err != nil {
		return err
	}
	return s.red.Expire(s.ctx, key, s.red.ttl).Err()
}

func (s *FullRedisCache[T, I]) Get(id I) (T, bool, error) {
	key := s.CacheKey()
	r, exists, err := s.red.HGetJson(key, id)
	if err != nil {
		return r, false, err
	}
	if exists {
		return r, true, nil
	}
	if err := s.Load(); err != nil {
		return r, false, err
	}
	s.red.Expires(key)
	return s.red.HGetJson(key, id)
}

func (s *FullRedisCache[T, I]) List(id ...I) ([]T, error) {
	key := s.CacheKey()
	count, err := s.red.Exists(s.ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		if err := s.Load(); err != nil {
			return nil, err
		}
	}
	s.red.Expires(key)
	return s.red.HMGetJson(key, id...)
}

func (s *FullRedisCache[T, I]) Create(r *T) error {
	if err := s.db.Create(r); err != nil {
		return err
	}
	return s.red.HSetJson(s.CacheKey(), *r)
}
func (s *FullRedisCache[T, I]) Save(r *T) error {
	_, exists, err := s.Get((*r).GetID())
	if err != nil {
		return err
	}
	if IsNullID((*r).GetID()) || !exists {
		if err := s.db.Create(r); err != nil {
			return err
		}
	} else {
		if err := s.db.Save(r); err != nil {
			return err
		}
	}
	return s.red.HSetJson(s.CacheKey(), *r)
}
func (s *FullRedisCache[T, I]) Update(id I, values interface{}) (int64, error) {
	if IsNullID(id) {
		return 0, nil
	}

	effectedRows, err := s.db.Update(id, values)
	if err != nil {
		return 0, err
	}
	r, _, err := s.db.Get(id)
	if err != nil {
		return 0, err
	}
	return effectedRows, s.red.HSetJson(s.CacheKey(), r)
}
func (s *FullRedisCache[T, I]) Delete(ids ...I) (int64, error) {
	rowsAffected, err := s.db.Delete(ids...)
	if err != nil {
		return 0, err
	}
	s.red.HDelJson(s.CacheKey(), ids...)
	return rowsAffected, err
}

func (s *FullRedisCache[T, I]) ListAll() ([]T, error) {
	key := s.CacheKey()
	count, err := s.red.Exists(s.ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		if err := s.Load(); err != nil {
			return nil, err
		}
	}
	s.red.Expires(key)
	return s.red.HGetAllJson(key)
}

func (s *FullRedisCache[T, I]) ClearCache(objs ...T) error {
	return s.red.Del(s.ctx, s.CacheKey()).Err()
}

func (s *FullRedisCache[T, I]) GetBy(index Index) (T, bool, error) {
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

func (s *FullRedisCache[T, I]) ListBy(index Index, orderBys OrderBys) ([]T, error) {
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
