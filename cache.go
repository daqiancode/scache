package scache

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type IDInt interface {
	~int | ~int16 | ~int32 | ~int64 | ~uint | ~uint16 | ~uint32 | ~uint64
}
type IDType interface {
	IDInt | ~string
}

func IsNullID[I IDType](id I) bool {
	var v interface{} = id
	switch x := v.(type) {
	case string:
		return len(x) == 0
	case int, int16, int32, int64, uint, uint16, uint32, uint64:
		return x == 0
	}
	return v == nil
}

// type CacheKeyMaker func(prefix, table string, indexes Indexes) string

type OrderBy struct {
	Field string
	Asc   bool
}

func (s OrderBy) String() string {
	if s.Asc {
		return s.Field + " ASC"
	}
	return s.Field + " DESC"
}

// type Indexes []Index
type OrderBys []OrderBy

func NewOrderBys(field string, asc bool) OrderBys {
	return []OrderBy{{Field: field, Asc: asc}}
}

func (s OrderBys) String() string {
	r := ""
	if len(s) == 0 {
		return r
	}
	for _, v := range s {
		r += v.String() + ","
	}
	return r[0 : len(r)-1]
}

func (s OrderBys) Add(field string, asc bool) OrderBys {
	return append(s, OrderBy{Field: field, Asc: asc})
}

type Index map[string]interface{}
type Indexes []Index

func NewIndex(field string, value interface{}) Index {
	return map[string]interface{}{field: value}
}

func (s Index) Fields() []string {
	fields := make([]string, len(s))
	i := 0
	for k := range s {
		fields[i] = k
		i++
	}
	return fields
}

func (s Index) Add(field string, value interface{}) Index {
	s[field] = value
	return s
}

func (s Indexes) Add(index Index) Indexes {
	return append(s, index)
}
func (s Indexes) Merge(indexes Indexes) Indexes {
	return append(s, indexes...)
}

type Table[I IDType] interface {
	//GetID return id field name & id value
	GetID() I
	ListIndexes() Indexes
}

type DBCRUD[T Table[I], I IDType] interface {
	//Creat create new record into dababase
	Create(obj *T) error
	//Save update if id exists or create new record
	Save(obj *T) error
	//Delete return (effectedrows,error)
	Delete(ids ...I) (int64, error)
	// values can be struct or map[string]interface{}, return (effectedrows,error)
	Update(id I, values interface{}) (int64, error)
	//get obj by id
	Get(id I) (T, bool, error)
	//list objs by ids
	List(ids ...I) ([]T, error)
	//get obj by index
	GetBy(index Index) (T, bool, error)
	//list objs by indexes
	ListBy(index Index, initOrders OrderBys) ([]T, error)
	//close lower clients
	Close() error
	//ListByUniqueInts list objs by unique index field in values
	ListByUniqueInts(field string, values []int64) ([]T, error)
	//ListByUniqueStrs list objs by unique index field in values
	ListByUniqueStrs(field string, values []string) ([]T, error)
}

// Cache
// 1. Primary key cache: eg. {table}/id/{id} ->  record
// 2.1 Index cache: eg1. {table}/uid/{uid}->  [id1,id2]
// 2.2 Index cache: eg2. {table}/uid/{uid}/type/{type} ->  [id1]
// 3. Index cache clear cache process, on index change 1. given indexes, 2. find related index cache keys,3. delete
type Cache[T Table[I], I IDType] interface {
	DBCRUD[T, I]
	//clear cache for objs
	ClearCache(objs ...T) error

	//for extending

	SetCacheKeyPrefix(prefix string)
	GetCacheKeyPrefix() string
	MakeCacheKey(index Index) string
	SetTableName(table string)
	GetTableName() string
	SetIdField(idField string)
	GetIdField() string
}

type FullCache[T Table[I], I IDType] interface {
	DBCRUD[T, I]
	ClearCache(objs ...T) error
	//Creat create new record into dababase
	// Create(obj *T) error
	// //Save update if id exists or create new record
	// Save(obj *T) error
	// //Delete return (effectedrows,error)
	// Delete(ids ...I) (int64, error)
	// // values can be struct or map[string]interface{}, return (effectedrows,error)
	// Update(id I, values interface{}) (int64, error)

	// //get obj by id
	// Get(id I) (T, bool, error)
	// //list objs by ids
	// List(ids ...I) ([]T, error)
	// //get obj by index
	// GetBy(index Index) (T, bool, error)
	// //list objs by indexes
	// ListBy(index Index, orderBys OrderBys) ([]T, error)
	//list all objs from db
	ListAll() ([]T, error)

	//close lower clients
	Close() error

	//for extending
	// SetCtx(ctx context.Context)
	// GetCtx() context.Context
	SetCacheKeyPrefix(prefix string)
	GetCacheKeyPrefix() string
	MakeCacheKey(index Index) string
	SetTableName(table string)
	GetTableName() string
	SetIdField(idField string)
	GetIdField() string
}

type CacheBase[T Table[I], I IDType] struct {
	prefix string
	table  string
	// serializer Serializer
	// cacheKeyMaker CacheKeyMaker
	idField string
	// indexFields [][]string
	// ctx context.Context
	// ttl         time.Duration
}

func NewCacheBase[T Table[I], I IDType](prefix, table, idField string) *CacheBase[T, I] {
	return &CacheBase[T, I]{
		prefix:  prefix,
		table:   table,
		idField: idField,
	}
}

//	func (s *CacheBase[T, I]) SetSerializer(serializer Serializer) {
//		s.serializer = serializer
//	}
//
//	func (s *CacheBase[T, I]) GetSerializer() Serializer {
//		return s.serializer
//	}
func (s *CacheBase[T, I]) SetCacheKeyPrefix(prefix string) {
	s.prefix = prefix
}
func (s *CacheBase[T, I]) GetCacheKeyPrefix() string {
	return s.prefix
}
func (s *CacheBase[T, I]) MakeCacheKey(index Index) string {
	r := s.prefix + "/" + s.table
	keys := index.Fields()
	sort.Strings(keys)
	for _, k := range keys {
		r += "/" + strings.ToLower(k) + "/" + Stringify(index[k], "null")
	}
	return r
}

func (s *CacheBase[T, I]) SetIdField(idField string) {
	s.idField = idField
}
func (s *CacheBase[T, I]) GetIdField() string {
	return s.idField
}
func (s *CacheBase[T, I]) SetTableName(table string) {
	s.table = table
}
func (s *CacheBase[T, I]) GetTableName() string {
	return s.table
}
func (s *CacheBase[T, I]) Close() error {
	return nil
}

//	func (s *CacheBase[T, I]) AddIndexFields(index []string) {
//		sort.Strings(index)
//		s.indexFields = append(s.indexFields, index)
//	}
//
//	func (s *CacheBase[T, I]) ListIndexFields() [][]string {
//		return s.indexFields
//	}
// func (s *CacheBase[T, I]) SetCtx(ctx context.Context) {
// 	s.ctx = ctx
// }
// func (s *CacheBase[T, I]) GetCtx() context.Context {
// 	return s.ctx
// }

func StringifyAtom(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		} else {
			return "false"
		}
	case int:
		return strconv.Itoa(v)
	case int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", value)
	case float32, float64:
		return fmt.Sprintf("%f", value)
	case time.Time:
		return v.Format(time.RFC3339)
	}
	return fmt.Sprintf("%#v", value)
}

func Stringify(value interface{}, null string) string {
	switch v := value.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		} else {
			return "false"
		}
	case int:
		return strconv.Itoa(v)
	case int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", value)
	case float32, float64:
		return fmt.Sprintf("%f", value)
	case time.Time:
		return v.Format(time.RFC3339)
	case sql.NullBool:
		if v.Valid {
			return Stringify(v.Bool, null)
		} else {
			return null
		}
	case sql.NullByte:
		if v.Valid {
			return Stringify(v.Byte, null)
		} else {
			return null
		}
	case sql.NullFloat64:
		if v.Valid {
			return Stringify(v.Float64, null)
		} else {
			return null
		}
	case sql.NullInt16:
		if v.Valid {
			return Stringify(v.Int16, null)
		} else {
			return null
		}
	case sql.NullInt32:
		if v.Valid {
			return Stringify(v.Int32, null)
		} else {
			return null
		}
	case sql.NullInt64:
		if v.Valid {
			return Stringify(v.Int64, null)
		} else {
			return null
		}
	case sql.NullString:
		if v.Valid {
			return Stringify(v.String, null)
		} else {
			return null
		}
	case sql.NullTime:
		if v.Valid {
			return Stringify(v.Time, null)
		} else {
			return null
		}
	}
	return fmt.Sprintf("%#v", value)
}

func UniqueStrings(strs []string) []string {
	m := make(map[string]bool)
	for _, v := range strs {
		m[v] = true
	}
	r := make([]string, len(m))
	i := 0
	for k := range m {
		r[i] = k
		i++
	}
	return r
}
