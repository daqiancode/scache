package mongoredis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/daqiancode/scache"
	"github.com/daqiancode/scache/mongoredis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Addr struct {
	Country string
	City    string
}
type Tag struct {
	Name string
}
type Commodity struct {
	Id       string `bson:"_id" json:"_id"`
	Name     string
	Category int
	Addr     Addr
	Tags     []Tag
}

func (s Commodity) GetID() string {
	return s.Id
}
func (s Commodity) ListIndexes() scache.Indexes {
	return scache.Indexes{}.Add(scache.Index{"category": s.Category}).Add(scache.Index{}.Add("addr.country", s.Addr.Country))
}
func getRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
}
func getMongoClient() *mongo.Client {
	clientOptions := options.Client().ApplyURI("mongodb://root:123456@127.0.0.1:27017")
	// clientOptions.Auth = &options.Credential{Username: "root", Password: "123456"}
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		panic(err)
	}
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		panic(err)
	}
	fmt.Println("Successfully connected and pinged.")
	return client
}

func TestBasic(t *testing.T) {

	client := getMongoClient()
	rm := mongoredis.NewMongoRedis[Commodity, string]("mongo", "test", "c1", "Id", client, getRedisClient(), 100*time.Second)
	defer rm.Close()
	c := Commodity{
		Id:       "62c7d675b562f2ded137850f",
		Name:     "mobile123",
		Category: 2,
		Tags:     []Tag{{Name: "tag1"}},
	}
	err := rm.Save(&c)
	assert.Nil(t, err)

	c1, exist, err := rm.Get("62c7d675b562f2ded137850")
	assert.Nil(t, err)
	fmt.Println(c1, exist)
	cs, err := rm.List("62c7d675b562f2ded137850f", "62c7d6aab9d0167a8b31c329", "62c7d675b562f2ded137850g")
	assert.Nil(t, err)
	fmt.Println(cs)
	// fmt.Printf("%#v\n", c)
	// c, _, err := rm.Get("622a35479486bba48b4e5170")
	// assert.Nil(t, err)
	// fmt.Printf("%#v\n", c)
	rm.Update("62c7d675b562f2ded137850f", map[string]interface{}{"addr.country": "uae", "tags.0.name": "gg"})
	// rm.Delete("62c7d675b562f2ded137850f")
	c2, exists, err := rm.GetBy(scache.NewIndex("tags.0.name", "gg"))
	assert.Nil(t, err)
	fmt.Println(c2, exists)

	c3, err := rm.ListBy(scache.NewIndex("name", "mobile"), nil)
	assert.Nil(t, err)
	fmt.Println(c3)
}

func TestCacheFull(t *testing.T) {
	rm := mongoredis.NewMongoRedisFull[Commodity, string]("mongo", "test", "c1", "Id", getMongoClient(), getRedisClient(), 100*time.Second)
	c1, exist, err := rm.Get("62c7d675b562f2ded137850f")
	assert.Nil(t, err)
	assert.True(t, exist)
	fmt.Println(c1)
}
