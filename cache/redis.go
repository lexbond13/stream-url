package main

import (
	"github.com/go-redis/redis"
	"time"
)

func initRedis() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
}

func existLock(key string) bool {

	i := redisCli.Exists(key).Val()
	if i <= 0 {
		return false
	}

	return true

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
