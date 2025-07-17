package redisdk

import (
	"context"
	"time"
)

func LPush(key string, value ...any) (int64, error) {
	return universalClient.LPush(context.Background(), key, value...).Result()
}

func BRPop(timeout time.Duration, key string) (string, error) {
	rts, err := universalClient.BRPop(context.Background(), timeout, key).Result()
	if err != nil {
		return "", err
	}
	if len(rts) == 2 {
		return rts[1], nil
	}
	return "", nil
}
