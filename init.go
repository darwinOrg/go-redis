package redisdk

import (
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

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
	if addr == "" {
		addr = os.Getenv("REDIS_ADDR")
	}

	redisCli = redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: os.Getenv("REDIS_USERNAME"),
		Password: os.Getenv("REDIS_PASSWORD"),
		PoolSize: getRedisPoolSize(),
	})
	universalClient = redisCli
}

func NewClient(addr string) *redis.Client {
	if addr == "" {
		addr = os.Getenv("REDIS_ADDR")
	}

	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: os.Getenv("REDIS_USERNAME"),
		Password: os.Getenv("REDIS_PASSWORD"),
		PoolSize: getRedisPoolSize(),
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

func getRedisPoolSize() int {
	poolSize := os.Getenv("REDIS_POOL_SIZE")
	if poolSize == "" {
		return 0
	}

	poolSizeInt, _ := strconv.Atoi(poolSize)
	return poolSizeInt
}
