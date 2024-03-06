package gormredis_test

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
			IgnoreRecordNotFoundError: true,        // Ignore IgnoreRecordNotFoundError error for logger
			Colorful:                  false,       // Disable color
		},
	)
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN: "root:123456@tcp(localhost:3306)/test?charset=utf8&parseTime=True&loc=Local",
	}), &gorm.Config{Logger: newLogger})
	if err != nil {
		panic(err)
	}
	db.Migrator().AutoMigrate(&Commodity{})
	return db
}

func getRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
}

type Commodity struct {
	Id         string
	Name       string
	CategoryId uint64
	UserId     int64
}

func (s Commodity) GetID() string {
	return s.Id
}
func (s Commodity) ListIndexes() scache.Indexes {
	return scache.Indexes{}.Add(scache.Index{}.Add("CategoryId", s.CategoryId)).Add(scache.Index{}.Add("Name", s.Name))
}
func createCache() scache.Cache[Commodity, string] {
	return gormredis.NewGormRedis[Commodity, string]("app", "commodity", "Id", GetDBClient(), getRedisClient(), 100*time.Second)

}
func createCacheFull() scache.FullCache[Commodity, string] {
	return gormredis.NewGormRedisFull[Commodity, string]("app", "commodity", "Id", GetDBClient(), getRedisClient(), 10*time.Second)

}

func TestGorm(t *testing.T) {
	db := GetDBClient()
	table := db.NamingStrategy.TableName("ProductDetail")
	fmt.Println(table)
	field := db.NamingStrategy.ColumnName("ProductDetail", "ProductId")
	fmt.Println(field)
	field1 := db.NamingStrategy.ColumnName("ProductDetail", "product_id")
	fmt.Println(field1)
}
func TestGormRedisMain(t *testing.T) {
	id := "1"
	ca := createCache()
	_, err := ca.Delete(id)
	assert.Nil(t, err)
	c, err := ca.Get(id)
	assert.Equal(t, scache.ErrRecordNotFound, err)
	assert.Equal(t, c.Id, "")
	d := Commodity{Id: id, Name: "tom", CategoryId: 1, UserId: 1}
	err = ca.Create(&d)
	assert.Nil(t, err)
	r1, err := ca.GetBy(scache.NewIndex("CategoryId", 1))
	assert.Nil(t, err)
	assert.Equal(t, id, r1.Id)
	r2, err := ca.GetBy(scache.NewIndex("CategoryId", 100))
	assert.Equal(t, scache.ErrRecordNotFound, err)
	assert.Equal(t, "", r2.Id)
	r3, err := ca.ListBy(scache.NewIndex("CategoryId", 100), nil)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(r3))
	_, err = ca.Update(id, map[string]any{"CategoryId": 100})
	assert.Nil(t, err)
	r2, err = ca.GetBy(scache.NewIndex("CategoryId", 100))
	assert.Nil(t, err)
	assert.Equal(t, id, r2.Id)

	r5, err := ca.Get("1")
	assert.Nil(t, err)
	assert.Equal(t, id, r5.Id)

	r4, err := ca.List("1", "100")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(r4))
	assert.Equal(t, id, r4[0].Id)

	ca.Delete("1")
	r6, err := ca.Get("1")
	assert.Equal(t, scache.ErrRecordNotFound, err)
	assert.Equal(t, "", r6.Id)
}

func TestCacheFull(t *testing.T) {
	s := createCacheFull()
	c, err := s.Get("1")
	assert.Nil(t, err)
	fmt.Println(c)
	fmt.Println(s.GetBy(scache.NewIndex("CategoryId", 1)))
	fmt.Println(s.GetBy(scache.NewIndex("CategoryId", 1)))
	fmt.Println(s.GetBy(scache.NewIndex("CategoryId", 2)))
	fmt.Println(s.GetBy(scache.NewIndex("CategoryId", 2)))
}

func TestListByUniqueInts(t *testing.T) {
	s := createCache()
	for i := 0; i < 3; i++ {

		c, err := s.ListByUniqueInts("CategoryId", []int64{1, 2})
		assert.Nil(t, err)
		assert.True(t, len(c) > 0)
		fmt.Println(i, c)
	}
}

func TestListByUniqueStrs(t *testing.T) {
	s := createCache()
	for i := 0; i < 1; i++ {
		c, err := s.ListByUniqueStrs("Name", []string{"tom", "Jerry"})
		assert.Nil(t, err)
		assert.True(t, len(c) > 0)
		fmt.Println(i, c)
	}
}

func TestUpdate(t *testing.T) {
	s := createCache()
	rowsAffected, err := s.Update("1", map[string]interface{}{"Name": "tom1", "UserId": 2})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), rowsAffected)
}
