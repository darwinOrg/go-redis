package redisdk

import "github.com/redis/go-redis/v9"

var redisCli *redis.Client
var clusterCli *redis.ClusterClient

func SetClient(client *redis.Client) {
	redisCli = client
}

func InitClient(addr string) {
	redisCli = redis.NewClient(&redis.Options{Addr: addr})
}

func NewClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: addr,
	})
}

func SetClusterClient(client *redis.ClusterClient) {
	clusterCli = client
}

func InitClusterClient(addrs []string) {
	clusterCli = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
}

func NewClusterClient(addrs []string) *redis.ClusterClient {
	return redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
}

func InitFailoverClient(masterName string, sentinelAddrs []string) {
	redisCli = redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs,
	})
}

func NewFailoverClient(masterName string, sentinelAddrs []string) *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs,
	})
}

func GetRedisClient() *redis.Client {
	return redisCli
}

func GetClusterClient() *redis.ClusterClient {
	return clusterCli
}
