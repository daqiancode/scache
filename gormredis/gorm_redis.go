package gormredis

import (
	"time"

	"github.com/daqiancode/scache"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

func NewGormRedis[T scache.Table[I], I scache.IDType](prefix, table, idField string, db *gorm.DB, red *redis.Client, ttl time.Duration) *scache.RedisCache[T, I] {
	rc := scache.NewRedisCache[T, I](prefix, table, idField, &Gorm[T, I]{db: db, table: table, idField: idField}, red, ttl)
	return rc
}
func NewGormRedisFull[T scache.Table[I], I scache.IDType](prefix, table, idField string, db *gorm.DB, red *redis.Client, ttl time.Duration) scache.FullCache[T, I] {
	rc := scache.NewFullRedisCache[T, I](prefix, table, idField, &Gorm[T, I]{db: db, table: table, idField: idField}, red, ttl)
	return rc
}

type Gorm[T scache.Table[I], I scache.IDType] struct {
	db      *gorm.DB
	table   string
	idField string
}

func (s *Gorm[T, I]) Close() error {
	return nil
}
func (s *Gorm[T, I]) DB() *gorm.DB {
	return s.db
}
func (s *Gorm[T, I]) Create(r *T) error {
	if err := s.db.Create(r).Error; err != nil {
		return err
	}
	return nil
}
func (s *Gorm[T, I]) Save(r *T) error {
	_, err := s.Get((*r).GetID())
	if err != nil && err != scache.ErrRecordNotFound {
		return err
	}
	if err == scache.ErrRecordNotFound {
		return s.Create(r)
	}
	return s.db.Save(r).Error
}
func (s *Gorm[T, I]) Update(id I, values interface{}) (int64, error) {
	old, err := s.Get(id)
	if err != nil {
		return 0, err
	}
	var rs *gorm.DB
	if vs, ok := values.(map[string]interface{}); ok {
		if len(vs) == 0 {
			return 0, nil
		}
	} else {
		rs = s.db.Model(&old).Updates(values)
	}
	if rs.Error != nil {
		return 0, rs.Error
	}
	return rs.RowsAffected, nil
}
func (s *Gorm[T, I]) Delete(ids ...I) (int64, error) {
	rs := s.db.Delete(new(T), ids)
	if rs.Error != nil {
		return 0, rs.Error
	}
	return rs.RowsAffected, nil
}
func (s *Gorm[T, I]) Get(id I) (T, error) {
	var r T
	if err := s.db.Where(map[string]interface{}{s.idField: id}).First(&r).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return r, scache.ErrRecordNotFound
		}
		return r, err
	}
	return r, nil
}
func (s *Gorm[T, I]) GetBy(index scache.Index) (T, error) {
	var r T
	index1 := make(scache.Index, len(index))
	for k, v := range index {
		index1[s.db.NamingStrategy.ColumnName(s.table, k)] = v
	}
	if err := s.db.Where(map[string]interface{}(index1)).First(&r).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return r, scache.ErrRecordNotFound
		}
		return r, err
	}
	return r, nil
}
func (s *Gorm[T, I]) List(ids ...I) ([]T, error) {
	var r []T
	err := s.db.Find(&r, ids).Error
	return r, err
}
func (s *Gorm[T, I]) ListBy(index scache.Index, initOrders scache.OrderBys) ([]T, error) {
	var r []T
	index1 := make(scache.Index, len(index))
	for k, v := range index {
		index1[s.db.NamingStrategy.ColumnName(s.table, k)] = v
	}
	for i, v := range initOrders {
		initOrders[i].Field = s.db.NamingStrategy.ColumnName(s.table, v.Field)
	}

	if err := s.db.Where(map[string]interface{}(index1)).Order(initOrders.String()).Find(&r).Error; err != nil {
		return nil, err
	}
	return r, nil
}

func (s *Gorm[T, I]) ListAll() ([]T, error) {
	var r []T
	if err := s.db.Find(&r).Error; err != nil {
		return nil, err
	}
	return r, nil
}

// ListIn list objs by index field in values
func (s *Gorm[T, I]) ListByUniqueInts(field string, values []int64) ([]T, error) {
	dbField := s.db.NamingStrategy.ColumnName(s.table, field)
	var r []T
	if err := s.db.Where(dbField+" in ?", values).Find(&r).Error; err != nil {
		return nil, err
	}
	return r, nil
}

// ListIn list objs by index field in values
func (s *Gorm[T, I]) ListByUniqueStrs(field string, values []string) ([]T, error) {
	dbField := s.db.NamingStrategy.ColumnName(s.table, field)
	var r []T
	if err := s.db.Where(dbField+" in ?", values).Find(&r).Error; err != nil {
		return nil, err
	}
	return r, nil
}
