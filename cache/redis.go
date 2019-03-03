package main

import (
	"github.com/go-redis/redis"
	"time"
)

var lockPrefix = ".lock"

func initRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
}

func existLock(key string) bool {
	i := redisCli.Exists(key + lockPrefix).Val()
	if i <= 0 {
		return false
	}

	return true
}

func redisSetLock(key string) error {
	return redisSet(key + lockPrefix, "lock")
}

func redisSetUnlock(key string) error {
	return redisDel(key + lockPrefix)
}

func redisGet(key string) (string, error) {
	return redisCli.Get(key).Result()
}

func redisSet(key string, val string) error {
	duration := time.Duration(randInt(config.MinTimeout, config.MaxTimeout)) * time.Second
	return redisCli.Set(key, val, duration).Err()
}

func redisDel(key string) error {
	return redisCli.Del(key).Err()
}
