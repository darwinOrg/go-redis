package redisdk

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

func XAdd(stream string, values any) (string, error) {
	return universalClient.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: values}).Result()
}

func XAddValues(stream string, values any) (string, error) {
	return universalClient.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: values, NoMkStream: true}).Result()
}

func XDel(stream string, ids ...string) (int64, error) {
	return universalClient.XDel(context.Background(), stream, ids...).Result()
}

func XGroupCreateMkStream(stream string, group string) (string, error) {
	return universalClient.XGroupCreateMkStream(context.Background(), stream, group, "$").Result()
}

func XGroupDestroy(stream string, group string) (int64, error) {
	return universalClient.XGroupDestroy(context.Background(), stream, group).Result()
}

func XReadGroup(stream string, group string, consumer string, block time.Duration, count int64) ([]redis.XStream, error) {
	return universalClient.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Block:    block,
		Count:    count,
	}).Result()
}

func XTrimMaxLen(stream string, maxLen int64) (int64, error) {
	return universalClient.XTrimMaxLen(context.Background(), stream, maxLen).Result()
}

func XAck(stream string, group string, messageId string) (int64, error) {
	return universalClient.XAck(context.Background(), stream, group, messageId).Result()
}
