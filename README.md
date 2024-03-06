# scache
A cache layer on top of ORM layer

## Installation
```shell
go get github.com/daqiancode/scache
#for gorm
go get github.com/daqiancode/scache/gormredis
#for mongodb
go get github.com/daqiancode/scache/mongoredis
```

## Idea
### 2 Types of Cache
1. Partial cache: load record on demands
2. Full cache: always load full data from database to cache

### Action
1. `Get`,`List`,`GetBy`,`List` will use cache(fetch from db if miss)
2. `Create`,`Delete`,`Update`,`Save` will clear the cache

### 2 type Cache content with redis
1. primary key -> obj, `Get`,`List` will use primary redis key, eg. `Get`: commodity/id/1 -> {id:3,name:"apply",category:1}
2. index key -> primary keys. eg.`ListBy` user/category/1 ->[3,4]

### Clear cache logic
1. Get related objects,eg. update(id,v), related objs is old record and new record after updated,`[old,new]`
2. Clear cache with id and index rediskey of related objs, `clearCache([old,new])`

## Support
1. Gorm, including MySQL, PostgreSQL, SQLite, SQL Server
2. Mongo

## Example
```go
import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/daqiancode/scache"
	"github.com/daqiancode/scache/gormredis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func GetDBClient() *gorm.DB {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second, // Slow SQL threshold
			LogLevel:                  logger.Info, // Log level
			IgnoreErrRecordNotFoundError: true,        // Ignore ErrErrRecordNotFound error for logger
			Colorful:                  false,       // Disable color
		},
	)
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN: "root:123456@tcp(localhost:3306)/test?charset=utf8&parseTime=True&loc=Local",
	}), &gorm.Config{Logger: newLogger})
	if err != nil {
		panic(err)
	}
	return db
}

func getRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
}

type Commodity struct {
	Id       string
	Name     string
	Category int
}

func (s Commodity) GetID() string {
	return s.Id
}
func (s Commodity) ListIndexes() scache.Indexes {
	return scache.Indexes{}.Add(scache.Index{}.Add("Category", s.Category))
}
func createCache() scache.Cache[Commodity, string] {
	return gormredis.NewGormRedis[Commodity, string]("app", "commodity", "Id", GetDBClient(), getRedisClient(), 10*time.Second)

}
func createCacheFull() *scache.FullRedisCache[Commodity, string] {
	return gormredis.NewGormRedisFull[Commodity, string]("app", "commodity", "Id", GetDBClient(), getRedisClient(), 10*time.Second)

}
func TestGormRedisMain(t *testing.T) {
	id := "1"
	ca := createCache()
	_, err := ca.Delete(id)
	assert.Nil(t, err)
	c, exists, err := ca.Get(id)
	assert.Nil(t, err)
	fmt.Println(exists)
	assert.False(t, exists)
	assert.Equal(t, c.Id, "")
	d := Commodity{Id: id, Name: "tom", Category: 1}
	err = ca.Create(&d)
	assert.Nil(t, err)
	r1, exists, err := ca.GetBy(scache.NewIndex("category", 1))
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.Equal(t, id, r1.Id)
	r2, exists, err := ca.GetBy(scache.NewIndex("category", 100))
	assert.Nil(t, err)
	assert.False(t, exists)
	assert.Equal(t, "", r2.Id)
	r3, err := ca.ListBy(scache.NewIndex("category", 100), nil)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(r3))
	r4, err := ca.List("1", "100")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(r4))
	assert.Equal(t, id, r4[0].Id)
}


```