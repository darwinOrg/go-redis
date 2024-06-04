package redisdk

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type BRPopCallback func(value string) error
type BRPopFunc func(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd

// RedisCli is abstraction for redis client, required commands only not all commands
type RedisCli interface {
	// Eval sends lua script to redis
	// args should be string, integer or float
	// returns string, int64, []any (elements can be string or int64)
	Eval(script string, keys []string, args ...any) (any, error)
	Set(key string, value any, expiration time.Duration) (string, error)
	// Get represents redis command GET
	// please NilErr when no such key in redis
	Get(key string) (string, error)
	Del(keys ...string) error
	HSet(key string, field string, value string) error
	HDel(key string, fields ...string) error
	SMembers(key string) ([]string, error)
	SRem(key string, members ...string) error
	ZAdd(key string, values map[string]float64) error
	ZRem(key string, fields ...string) error
	ZCard(key string) (int64, error)
	LLen(key string) (int64, error)

	PExpire(stream string, expiration time.Duration) (bool, error)
	SetNX(key string, value string, ttl time.Duration) (bool, error)

	// Publish used for monitor only
	Publish(channel string, payload string) error
	// Subscribe used for monitor only
	// returns: payload channel, subscription closer, error; the subscription closer should close payload channel as well
	Subscribe(channel string) (payloads <-chan string, close func(), err error)

	XAdd(args *redis.XAddArgs) (string, error)
	XDel(stream string, ids ...string) (int64, error)
	XGroupCreateMkStream(stream string, group string, start string) (string, error)
	XGroupDestroy(stream string, group string) (int64, error)
	XReadGroup(args *redis.XReadGroupArgs) ([]redis.XStream, error)
	XTrimMaxLen(stream string, maxLen int64) (int64, error)
	XAck(stream string, group string, messageId string) (int64, error)

	LPush(key string, value ...interface{}) (int64, error)
	BRPop(ctx context.Context, key string, timeout time.Duration, callback BRPopCallback)
}

type redisV9Wrapper struct {
	inner *redis.Client
}

func (r *redisV9Wrapper) Eval(script string, keys []string, args ...any) (any, error) {
	ret, err := r.inner.Eval(context.Background(), script, keys, args...).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) Set(key string, value any, expiration time.Duration) (string, error) {
	ret, err := r.inner.Set(context.Background(), key, value, expiration).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) Get(key string) (string, error) {
	ret, err := r.inner.Get(context.Background(), key).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) Del(keys ...string) error {
	return wrapErr(r.inner.Del(context.Background(), keys...).Err())
}

func (r *redisV9Wrapper) HSet(key string, field string, value string) error {
	return wrapErr(r.inner.HSet(context.Background(), key, field, value).Err())
}

func (r *redisV9Wrapper) HDel(key string, fields ...string) error {
	return wrapErr(r.inner.HDel(context.Background(), key, fields...).Err())
}

func (r *redisV9Wrapper) SMembers(key string) ([]string, error) {
	ret, err := r.inner.SMembers(context.Background(), key).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) SRem(key string, members ...string) error {
	members2 := make([]any, len(members))
	for i, v := range members {
		members2[i] = v
	}
	return wrapErr(r.inner.SRem(context.Background(), key, members2...).Err())
}

func (r *redisV9Wrapper) ZAdd(key string, values map[string]float64) error {
	var zs []redis.Z
	for member, score := range values {
		zs = append(zs, redis.Z{
			Score:  score,
			Member: member,
		})
	}
	return wrapErr(r.inner.ZAdd(context.Background(), key, zs...).Err())
}

func (r *redisV9Wrapper) ZRem(key string, members ...string) error {
	members2 := make([]any, len(members))
	for i, v := range members {
		members2[i] = v
	}
	return wrapErr(r.inner.ZRem(context.Background(), key, members2...).Err())
}

func (r *redisV9Wrapper) ZCard(key string) (int64, error) {
	return r.inner.ZCard(context.Background(), key).Result()
}

func (r *redisV9Wrapper) LLen(key string) (int64, error) {
	return r.inner.LLen(context.Background(), key).Result()
}

func (r *redisV9Wrapper) PExpire(stream string, expiration time.Duration) (bool, error) {
	ret, err := r.inner.PExpire(context.Background(), stream, expiration).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) SetNX(key string, value string, ttl time.Duration) (bool, error) {
	ret, err := r.inner.SetNX(context.Background(), key, value, ttl).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) Publish(channel string, payload string) error {
	return r.inner.Publish(context.Background(), channel, payload).Err()
}

func (r *redisV9Wrapper) Subscribe(channel string) (<-chan string, func(), error) {
	sub := r.inner.Subscribe(context.Background(), channel)
	closeFn := func() {
		_ = sub.Close()
	}
	resultChan := make(chan string) // sub.Channel() has its own buffer
	go func() {
		for msg := range sub.Channel() {
			resultChan <- msg.Payload
		}
	}()

	return resultChan, closeFn, nil
}

func (r *redisV9Wrapper) XAdd(args *redis.XAddArgs) (string, error) {
	ret, err := r.inner.XAdd(context.Background(), args).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) XDel(stream string, ids ...string) (int64, error) {
	ret, err := r.inner.XDel(context.Background(), stream, ids...).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) XGroupCreateMkStream(stream string, group string, start string) (string, error) {
	ret, err := r.inner.XGroupCreateMkStream(context.Background(), stream, group, start).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) XGroupDestroy(stream string, group string) (int64, error) {
	ret, err := r.inner.XGroupDestroy(context.Background(), stream, group).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) XReadGroup(args *redis.XReadGroupArgs) ([]redis.XStream, error) {
	ret, err := r.inner.XReadGroup(context.Background(), args).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) XTrimMaxLen(stream string, maxLen int64) (int64, error) {
	ret, err := r.inner.XTrimMaxLen(context.Background(), stream, maxLen).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) XAck(stream string, group string, messageId string) (int64, error) {
	ret, err := r.inner.XAck(context.Background(), stream, group, messageId).Result()
	return ret, wrapErr(err)
}

func (r *redisV9Wrapper) LPush(key string, value ...interface{}) (int64, error) {
	return r.inner.LPush(context.Background(), key, value...).Result()
}

func (r *redisV9Wrapper) BRPop(ctx context.Context, key string, timeout time.Duration, callback BRPopCallback) {
	innerBRPop(ctx, r.inner.BRPop, key, timeout, callback)
}

type redisClusterWrapper struct {
	inner *redis.ClusterClient
}

func (r *redisClusterWrapper) Eval(script string, keys []string, args ...any) (any, error) {
	ret, err := r.inner.Eval(context.Background(), script, keys, args...).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) Set(key string, value any, expiration time.Duration) (string, error) {
	ret, err := r.inner.Set(context.Background(), key, value, expiration).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) Get(key string) (string, error) {
	ret, err := r.inner.Get(context.Background(), key).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) Del(keys ...string) error {
	return wrapErr(r.inner.Del(context.Background(), keys...).Err())
}

func (r *redisClusterWrapper) HSet(key string, field string, value string) error {
	return wrapErr(r.inner.HSet(context.Background(), key, field, value).Err())
}

func (r *redisClusterWrapper) HDel(key string, fields ...string) error {
	return wrapErr(r.inner.HDel(context.Background(), key, fields...).Err())
}

func (r *redisClusterWrapper) SMembers(key string) ([]string, error) {
	ret, err := r.inner.SMembers(context.Background(), key).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) SRem(key string, members ...string) error {
	members2 := make([]any, len(members))
	for i, v := range members {
		members2[i] = v
	}
	return wrapErr(r.inner.SRem(context.Background(), key, members2...).Err())
}

func (r *redisClusterWrapper) ZAdd(key string, values map[string]float64) error {
	var zs []redis.Z
	for member, score := range values {
		zs = append(zs, redis.Z{
			Score:  score,
			Member: member,
		})
	}
	return wrapErr(r.inner.ZAdd(context.Background(), key, zs...).Err())
}

func (r *redisClusterWrapper) ZRem(key string, members ...string) error {
	members2 := make([]any, len(members))
	for i, v := range members {
		members2[i] = v
	}
	return wrapErr(r.inner.ZRem(context.Background(), key, members2...).Err())
}

func (r *redisClusterWrapper) ZCard(key string) (int64, error) {
	return r.inner.ZCard(context.Background(), key).Result()
}

func (r *redisClusterWrapper) LLen(key string) (int64, error) {
	return r.inner.LLen(context.Background(), key).Result()
}

func (r *redisClusterWrapper) PExpire(stream string, expiration time.Duration) (bool, error) {
	ret, err := r.inner.PExpire(context.Background(), stream, expiration).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) SetNX(key string, value string, ttl time.Duration) (bool, error) {
	ret, err := r.inner.SetNX(context.Background(), key, value, ttl).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) Publish(channel string, payload string) error {
	return r.inner.Publish(context.Background(), channel, payload).Err()
}

func (r *redisClusterWrapper) Subscribe(channel string) (<-chan string, func(), error) {
	sub := r.inner.Subscribe(context.Background(), channel)
	closeFn := func() {
		_ = sub.Close()
	}
	resultChan := make(chan string) // sub.Channel() has its own buffer
	go func() {
		for msg := range sub.Channel() {
			resultChan <- msg.Payload
		}
	}()

	return resultChan, closeFn, nil
}

func (r *redisClusterWrapper) XAdd(args *redis.XAddArgs) (string, error) {
	ret, err := r.inner.XAdd(context.Background(), args).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) XDel(stream string, ids ...string) (int64, error) {
	ret, err := r.inner.XDel(context.Background(), stream, ids...).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) XGroupCreateMkStream(stream string, group string, start string) (string, error) {
	ret, err := r.inner.XGroupCreateMkStream(context.Background(), stream, group, start).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) XGroupDestroy(stream string, group string) (int64, error) {
	ret, err := r.inner.XGroupDestroy(context.Background(), stream, group).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) XReadGroup(args *redis.XReadGroupArgs) ([]redis.XStream, error) {
	ret, err := r.inner.XReadGroup(context.Background(), args).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) XTrimMaxLen(stream string, maxLen int64) (int64, error) {
	ret, err := r.inner.XTrimMaxLen(context.Background(), stream, maxLen).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) XAck(stream string, group string, messageId string) (int64, error) {
	ret, err := r.inner.XAck(context.Background(), stream, group, messageId).Result()
	return ret, wrapErr(err)
}

func (r *redisClusterWrapper) LPush(key string, value ...interface{}) (int64, error) {
	return r.inner.LPush(context.Background(), key, value...).Result()
}

func (r *redisClusterWrapper) BRPop(ctx context.Context, key string, timeout time.Duration, callback BRPopCallback) {
	innerBRPop(ctx, r.inner.BRPop, key, timeout, callback)
}

func innerBRPop(ctx context.Context, brPopFunc BRPopFunc, key string, timeout time.Duration, callback BRPopCallback) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				results, err := brPopFunc(context.Background(), timeout, key).Result()
				if err != nil {
					fmt.Println(err)
					time.Sleep(time.Second)
					continue
				}
				if len(results) == 2 {
					_ = callback(results[1])
				}
			}
		}
	}()
}
