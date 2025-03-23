package redisdk

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

func Set(key string, value any, expiration time.Duration) (val string, err error) {
	if redisCli != nil {
		return redisCli.Set(context.Background(), key, value, expiration).Result()
	} else {
		return clusterCli.Set(context.Background(), key, value, expiration).Result()
	}
}

func Get(key string) (string, error) {
	if redisCli != nil {
		return redisCli.Get(context.Background(), key).Result()
	} else {
		return clusterCli.Get(context.Background(), key).Result()
	}
}

func Expire(key string, expiration time.Duration) (bool, error) {
	if redisCli != nil {
		return redisCli.Expire(context.Background(), key, expiration).Result()
	} else {
		return clusterCli.Expire(context.Background(), key, expiration).Result()
	}
}

func XAdd(stream string, values any) (string, error) {
	if redisCli != nil {
		return redisCli.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: values}).Result()
	} else {
		return clusterCli.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: values}).Result()
	}
}

func PExpire(stream string, expiration time.Duration) error {
	var success bool
	var err error
	if redisCli != nil {
		success, err = redisCli.PExpire(context.Background(), stream, expiration).Result()
	} else {
		success, err = clusterCli.PExpire(context.Background(), stream, expiration).Result()
	}
	if err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("failed to set expiration for stream %s", stream)
	}
	return nil
}

func XAddValues(stream string, values any) (string, error) {
	if redisCli != nil {
		return redisCli.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: values, NoMkStream: true}).Result()
	} else {
		return clusterCli.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: values, NoMkStream: true}).Result()
	}
}

func XDel(stream string, ids ...string) (int64, error) {
	if redisCli != nil {
		return redisCli.XDel(context.Background(), stream, ids...).Result()
	} else {
		return clusterCli.XDel(context.Background(), stream, ids...).Result()
	}
}

func XGroupCreateMkStream(stream string, group string) (string, error) {
	if redisCli != nil {
		return redisCli.XGroupCreateMkStream(context.Background(), stream, group, "$").Result()
	} else {
		return clusterCli.XGroupCreateMkStream(context.Background(), stream, group, "$").Result()
	}
}

func XGroupDestroy(stream string, group string) (int64, error) {
	if redisCli != nil {
		return redisCli.XGroupDestroy(context.Background(), stream, group).Result()
	} else {
		return clusterCli.XGroupDestroy(context.Background(), stream, group).Result()
	}
}

func XReadGroup(stream string, group string, consumer string, block time.Duration, count int64) ([]redis.XStream, error) {
	if redisCli != nil {
		return redisCli.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Block:    block,
			Count:    count,
		}).Result()
	} else {
		return clusterCli.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Block:    block,
			Count:    count,
		}).Result()
	}
}

func XTrimMaxLen(stream string, maxLen int64) (int64, error) {
	if redisCli != nil {
		return redisCli.XTrimMaxLen(context.Background(), stream, maxLen).Result()
	} else {
		return clusterCli.XTrimMaxLen(context.Background(), stream, maxLen).Result()
	}
}

func XAck(stream string, group string, messageId string) (int64, error) {
	if redisCli != nil {
		return redisCli.XAck(context.Background(), stream, group, messageId).Result()
	} else {
		return clusterCli.XAck(context.Background(), stream, group, messageId).Result()
	}
}

func Del(keys ...string) (int64, error) {
	if redisCli != nil {
		return redisCli.Del(context.Background(), keys...).Result()
	} else {
		return clusterCli.Del(context.Background(), keys...).Result()
	}
}

func SetNX(lockKey string, ttl time.Duration) (bool, error) {
	if redisCli != nil {
		return redisCli.SetNX(context.Background(), lockKey, "locked", ttl).Result()
	} else {
		return clusterCli.SetNX(context.Background(), lockKey, "locked", ttl).Result()
	}
}

func Eval(script string, keys []string, args ...any) (any, error) {
	if redisCli != nil {
		return redisCli.Eval(context.Background(), script, keys, args...).Result()
	} else {
		return clusterCli.Eval(context.Background(), script, keys, args...).Result()
	}
}

func AcquireLock(lockKey string, ttl time.Duration) (bool, error) {
	var success bool
	var err error
	if redisCli != nil {
		success, err = redisCli.SetNX(context.Background(), lockKey, "locked", ttl).Result()
	} else {
		success, err = clusterCli.SetNX(context.Background(), lockKey, "locked", ttl).Result()
	}
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
	var res any
	var err error
	if redisCli != nil {
		res, err = redisCli.Eval(context.Background(), luaScript, []string{lockKey}, []any{"locked"}).Result()
	} else {
		res, err = clusterCli.Eval(context.Background(), luaScript, []string{lockKey}, []any{"locked"}).Result()
	}
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	if res.(int64) == 0 {
		return fmt.Errorf("lock was not held")
	}
	return nil
}

func LPush(key string, value ...interface{}) (int64, error) {
	if redisCli != nil {
		return redisCli.LPush(context.Background(), key, value...).Result()
	} else {
		return clusterCli.LPush(context.Background(), key, value...).Result()
	}
}

func BRPop(timeout time.Duration, key string) (string, error) {
	var rts []string
	var err error
	if redisCli != nil {
		rts, err = redisCli.BRPop(context.Background(), timeout, key).Result()
	} else {
		rts, err = clusterCli.BRPop(context.Background(), timeout, key).Result()
	}
	if err != nil {
		return "", err
	}
	if len(rts) == 2 {
		return rts[1], nil
	}
	return "", nil
}
