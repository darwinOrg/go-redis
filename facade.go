package redisdk

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

func Set(key string, value any, expiration time.Duration) (string, error) {
	return redisCli.Set(key, value, expiration)
}

func Get(key string) (string, error) {
	return redisCli.Get(key)
}

func XAdd(stream string, values any) (string, error) {
	return redisCli.XAdd(&redis.XAddArgs{
		Stream: stream,
		Values: values,
	})
}

func PExpire(stream string, expiration time.Duration) (bool, error) {
	success, err := redisCli.PExpire(stream, expiration)
	if err != nil {
		return false, err
	}
	if !success {
		return false, fmt.Errorf("failed to set expiration for stream %s", stream)
	}

	return true, nil
}

func XAddValues(stream string, values any) (string, error) {
	return redisCli.XAdd(&redis.XAddArgs{
		Stream:     stream,
		Values:     values,
		NoMkStream: true,
	})
}

func XDel(stream string, ids ...string) (int64, error) {
	return redisCli.XDel(stream, ids...)
}

func XGroupCreateMkStream(stream string, group string) (string, error) {
	return redisCli.XGroupCreateMkStream(stream, group, "$")
}

func XGroupDestroy(stream string, group string) (int64, error) {
	return redisCli.XGroupDestroy(stream, group)
}

func XReadGroup(stream string, group string, consumer string, block time.Duration, count int64) ([]redis.XStream, error) {
	return redisCli.XReadGroup(&redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Block:    block,
		Count:    count,
	})
}

func XTrimMaxLen(stream string, maxLen int64) (int64, error) {
	return redisCli.XTrimMaxLen(stream, maxLen)
}

func XAck(stream string, group string, messageId string) (int64, error) {
	return redisCli.XAck(stream, group, messageId)
}

func Del(keys ...string) error {
	return redisCli.Del(keys...)
}

func AcquireLock(lockKey string, ttl time.Duration) (bool, error) {
	success, err := redisCli.SetNX(lockKey, "locked", ttl)
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
	res, err := redisCli.Eval(luaScript, []string{lockKey}, []any{"locked"})
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	if res.(int64) == 0 {
		return fmt.Errorf("lock was not held")
	}
	return nil
}
