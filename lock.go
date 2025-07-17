package redisdk

import (
	"context"
	"fmt"
	"time"
)

func AcquireLock(lockKey string, ttl time.Duration) (bool, error) {
	ok, err := universalClient.SetNX(context.Background(), lockKey, "locked", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock: %w", err)
	}
	return ok, nil
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
	res, err := universalClient.Eval(context.Background(), luaScript, []string{lockKey}, []any{"locked"}).Result()
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	if res.(int64) == 0 {
		return fmt.Errorf("lock was not held")
	}
	return nil
}
