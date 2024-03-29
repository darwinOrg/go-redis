package sand_river_sdk

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

var rdb *redis.Client

func InitClient(addr string) {
	rdb = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
}

func Set(key string, value interface{}, expiration time.Duration) (string, error) {
	return rdb.Set(context.TODO(), key, value, expiration).Result()
}

func Get(key string) (string, error) {
	return rdb.Get(context.TODO(), key).Result()
}

func XAdd(stream string, values interface{}) (string, error) {
	return rdb.XAdd(context.TODO(), &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Result()
}

func XGroupCreateMkStream(stream string, group string) (string, error) {
	return rdb.XGroupCreateMkStream(context.TODO(), stream, group, "$").Result()
}

func XGroupDestroy(stream string, group string) (int64, error) {
	return rdb.XGroupDestroy(context.TODO(), stream, group).Result()
}

func XReadGroup(stream string, group string, consumer string, block time.Duration, count int64) ([]redis.XStream, error) {
	return rdb.XReadGroup(context.TODO(), &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Block:    block,
		Count:    count,
	}).Result()
}

func XAck(stream string, group string, messageId string) (int64, error) {
	return rdb.XAck(context.TODO(), stream, group, messageId).Result()
}

func Del(keys ...string) (int64, error) {
	return rdb.Del(context.TODO(), keys...).Result()
}
