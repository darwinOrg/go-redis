package sand_river_sdk

import (
	"context"
	"fmt"
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

func AcquireLock(lockKey string, ttl time.Duration) (bool, error) {
	success, err := rdb.SetNX(context.Background(), lockKey, "locked", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock: %w", err)
	}
	return success, nil
}

func ReleaseLock(lockKey string) error {
	// 为了避免误删其他客户端的锁，可以使用Lua脚本来保证原子性
	luaScript := `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
	`
	res, err := rdb.Eval(context.Background(), luaScript, []string{lockKey}, "locked").Result()
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	if res.(int64) == 0 {
		return fmt.Errorf("lock was not held")
	}
	return nil
}
