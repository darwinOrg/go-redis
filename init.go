package redisdk

import "github.com/redis/go-redis/v9"

var (
	redisCli        *redis.Client
	clusterCli      *redis.ClusterClient
	universalClient redis.UniversalClient
)

func SetClient(client *redis.Client) {
	redisCli = client
	universalClient = redisCli
}

func InitClient(addr string) {
	redisCli = redis.NewClient(&redis.Options{Addr: addr})
	universalClient = redisCli
}

func NewClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: addr,
	})
}

func SetClusterClient(client *redis.ClusterClient) {
	clusterCli = client
	universalClient = clusterCli
}

func InitClusterClient(addrs []string) {
	clusterCli = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	universalClient = clusterCli
}

func NewClusterClient(addrs []string) *redis.ClusterClient {
	return redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
}

func InitFailoverClient(masterName string, sentinelAddrs []string) {
	redisCli = redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs,
	})
	universalClient = redisCli
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

func GetUniversalClient() redis.UniversalClient {
	return universalClient
}
