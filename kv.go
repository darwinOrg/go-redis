package redisdk

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

func Set(key string, value any, expiration time.Duration) (string, error) {
	return universalClient.Set(context.Background(), key, value, expiration).Result()
}

func Get(key string) (string, error) {
	val, err := universalClient.Get(context.Background(), key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}
	return val, nil
}

func Expire(key string, expiration time.Duration) (bool, error) {
	return universalClient.Expire(context.Background(), key, expiration).Result()
}

func PExpire(stream string, expiration time.Duration) error {
	ok, err := universalClient.PExpire(context.Background(), stream, expiration).Result()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("failed to set expiration for stream %s", stream)
	}
	return nil
}

func Del(keys ...string) (int64, error) {
	return universalClient.Del(context.Background(), keys...).Result()
}

func SetNX(key string, value any, expiration time.Duration) (bool, error) {
	return universalClient.SetNX(context.Background(), key, value, expiration).Result()
}

func Eval(script string, keys []string, args ...any) (any, error) {
	return universalClient.Eval(context.Background(), script, keys, args...).Result()
}
