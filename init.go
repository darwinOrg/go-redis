package redisdk

import "github.com/redis/go-redis/v9"

var redisCli RedisCli

func SetClient(client *redis.Client) {
	redisCli = &redisV9Wrapper{
		inner: client,
	}
}

func InitClient(addr string) {
	redisCli = &redisV9Wrapper{redis.NewClient(&redis.Options{
		Addr: addr,
	})}
}

func SetClusterClient(client *redis.ClusterClient) {
	redisCli = &redisClusterWrapper{
		inner: client,
	}
}

func InitClusterClient(addrs []string) {
	redisCli = &redisClusterWrapper{redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addrs,
	})}
}

func InitFailoverClient(masterName string, sentinelAddrs []string) {
	redisCli = &redisV9Wrapper{redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs,
	})}
}
