package scache

import (
	"context"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
)

var RdisOpTimeout = 30 * time.Second

type Serializer interface {
	Marshal(obj interface{}) (string, error)
	Unmarshal(data string, objRef interface{}) error
}

var json = jsoniter.Config{EscapeHTML: false, ObjectFieldMustBeSimpleString: true}.Froze()

type JsonSerializer struct {
}

func (s *JsonSerializer) Marshal(obj interface{}) (string, error) {
	return json.MarshalToString(obj)
}
func (s *JsonSerializer) Unmarshal(data string, objRef interface{}) error {
	return json.UnmarshalFromString(data, objRef)
}

type RedisJson[T any] struct {
	*redis.Client
	serializer Serializer
	ttl        time.Duration
}

func NewRedisJson[T any](client *redis.Client, ttl time.Duration) *RedisJson[T] {
	return &RedisJson[T]{
		Client:     client,
		serializer: &JsonSerializer{},
		ttl:        ttl,
	}
}

func (s *RedisJson[T]) GetJson(key string) (T, error) {
	var r T
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	y, err := s.Get(ctx, key).Result()
	if err != nil {
		return r, err
	}
	err = s.serializer.Unmarshal(y, &r)
	return r, err
}

func (s *RedisJson[T]) SetJson(key string, obj T) error {

	y, err := s.serializer.Marshal(obj)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	return s.SetEx(ctx, key, y, s.ttl).Err()
}

func (s *RedisJson[T]) MSetJson(objMap map[string]interface{}) error {
	if len(objMap) == 0 {
		return nil
	}
	objJsonMap := make(map[string]string, len(objMap))
	var err error
	keys := make([]string, len(objMap))
	i := 0
	for k, v := range objMap {
		objJsonMap[k], err = s.serializer.Marshal(v)
		if err != nil {
			return err
		}
		keys[i] = k
		i++
	}
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	err = s.MSet(ctx, objJsonMap).Err()
	if err != nil {
		return err
	}
	return s.Expires(keys...)
}

func (s *RedisJson[T]) Expires(keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	p := s.Pipeline()
	var err error
	for _, v := range keys {
		err = p.Expire(ctx, v, s.ttl).Err()
		if err != nil {
			return err
		}
	}
	_, err = p.Exec(ctx)
	return err

}

func (s *RedisJson[T]) SetNull(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	return s.SetEx(ctx, key, "null", s.ttl).Err()
}

func (s *RedisJson[T]) MSetNull(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	p := s.Pipeline()
	var err error
	for _, v := range keys {
		err = p.SetEx(ctx, v, "null", s.ttl).Err()
		if err != nil {
			return err
		}
	}
	_, err = p.Exec(ctx)
	return err
}

func (s *RedisJson[T]) MGetJson(keys []string) ([]T, []int, error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	vs, err := s.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}
	var missedIndexes []int
	r := make([]T, len(keys))
	for i, v := range vs {
		var t T
		if v == nil {
			missedIndexes = append(missedIndexes, i)
			r[i] = t
			continue
		}

		err = s.serializer.Unmarshal(v.(string), &t)
		if err != nil {
			return nil, missedIndexes, err
		}
		r[i] = t
	}
	for _, key := range keys {
		err = s.Expire(ctx, key, s.ttl).Err()
		if err != nil {
			return r, missedIndexes, err
		}
	}
	return r, missedIndexes, nil

}

type RedisHashJson[T Table[I], I IDType] struct {
	*RedisJson[T]
	*redis.Client
	serializer Serializer
	ctx        context.Context
	ttl        time.Duration
}

func NewRedisHashJson[T Table[I], I IDType](client *redis.Client, ttl time.Duration) *RedisHashJson[T, I] {
	return &RedisHashJson[T, I]{
		RedisJson:  NewRedisJson[T](client, ttl),
		Client:     client,
		serializer: &JsonSerializer{},
		ctx:        context.Background(),
		ttl:        ttl,
	}
}

func (s *RedisHashJson[T, I]) HGetJson(key string, id I) (T, error) {
	idStr := Stringify(id, "")
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	var r T
	raw, err := s.HGet(ctx, key, idStr).Result()
	if err != nil {

		return r, err
	}
	err = s.serializer.Unmarshal(raw, &r)
	return r, err
}

func (s *RedisHashJson[T, I]) HGetAllJson(key string) ([]T, error) {
	var r []T
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	raw, err := s.HGetAll(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return r, nil
		}
		return r, err
	}
	for _, v := range raw {
		var t T
		err = s.serializer.Unmarshal(v, &t)
		if err != nil {
			return r, nil
		}
		r = append(r, t)
	}
	return r, nil
}

func (s *RedisHashJson[T, I]) HMGetJson(key string, ids ...I) ([]T, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	idStrs := make([]string, len(ids))
	for i, v := range ids {
		idStrs[i] = Stringify(v, "")
	}
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	var r []T
	raw, err := s.HMGet(ctx, key, idStrs...).Result()
	if err != nil {
		if err == redis.Nil {
			return r, nil
		}
		return r, err
	}
	for _, v := range raw {
		if v == nil {
			continue
		}
		var t T
		err = s.serializer.Unmarshal(v.(string), &t)
		if err != nil {
			return r, nil
		}
		r = append(r, t)
	}
	return r, nil
}

func (s *RedisHashJson[T, I]) HSetJson(key string, objs ...T) error {
	if len(objs) == 0 {
		return nil
	}
	var err error
	args := make([]string, len(objs)*2)
	for k, v := range objs {
		args[2*k] = Stringify(v.GetID(), "")
		args[2*k+1], err = s.serializer.Marshal(v)
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	return s.HSet(ctx, key, args).Err()
}

func (s *RedisHashJson[T, I]) HDelJson(key string, ids ...I) error {
	if len(ids) == 0 {
		return nil
	}
	idStrs := make([]string, len(ids))
	for i, v := range ids {
		idStrs[i] = Stringify(v, "")
	}
	ctx, cancel := context.WithTimeout(context.Background(), RdisOpTimeout)
	defer cancel()
	return s.HDel(ctx, key, idStrs...).Err()
}
