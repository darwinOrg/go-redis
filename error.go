package redisdk

import (
	"errors"
	"github.com/redis/go-redis/v9"
)

// NilErr represents redis nil
var NilErr = errors.New("nil")

func wrapErr(err error) error {
	if err == redis.Nil {
		return NilErr
	}
	return err
}
