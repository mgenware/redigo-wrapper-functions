package mredis

import (
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"
)

// RedisWrapper wraps a redigo pool objects(redis.Pool) internally, providing a set of commonly used functions.
type RedisWrapper struct {
	pool *redis.Pool
}

// NewRedisWrapperFromPool returns an instance of RedisWrapper from a redis.Pool object.
func NewRedisWrapperFromPool(pool *redis.Pool) *RedisWrapper {
	return &RedisWrapper{pool: pool}
}

// NewRedisWrapperFromTcpConn returns an instance of RedisWrapper.
func NewRedisWrapperFromTcpConn(dialAddress string, maxIdle, maxActive int, dleTimeout time.Duration) *RedisWrapper {
	pool := &redis.Pool{
		MaxIdle:   maxIdle,
		MaxActive: maxActive,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", dialAddress)
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}

	return NewRedisWrapperFromPool(pool)
}

// Destroy closes the internal redis.Pool object.
func (store *RedisWrapper) Destroy() error {
	return store.pool.Close()
}

// SetStringValue calls SET command with a string value.
func (store *RedisWrapper) SetStringValue(key string, val string, expires int) error {
	if expires > 0 {
		return store.setValueWithTimeoutInternal(key, val, expires)
	}
	return store.setValueInternal(key, val)
}

// GetStringValue returns the result of GET command and converts it to string.
func (store *RedisWrapper) GetStringValue(key string) (string, error) {
	c := store.pool.Get()
	defer c.Close()

	return redis.String(c.Do("GET", key))
}

// RemoveValue calls DEL command.
func (store *RedisWrapper) RemoveValue(key string) error {
	c := store.pool.Get()
	defer c.Close()

	_, err := c.Do("DEL", key)
	return err
}

// Clear calls FLUSHALL command.
func (store *RedisWrapper) Clear() error {
	c := store.pool.Get()
	defer c.Close()

	_, err := c.Do("FLUSHALL")
	return err
}

// Ping calls PING commands.
func (store *RedisWrapper) Ping() error {
	c := store.pool.Get()
	defer c.Close()

	res, err := redis.String(c.Do("PING"))
	if err != nil {
		return err
	}
	if res != "PONG" {
		return errors.New("Redis responded with " + res)
	}
	return nil
}

/*** Internal functions ***/
func (store *RedisWrapper) setValueInternal(key string, val interface{}) error {
	c := store.pool.Get()
	defer c.Close()

	_, err := c.Do("SET", key, val)
	return err
}

func (store *RedisWrapper) setValueWithTimeoutInternal(key string, val interface{}, expires int) error {
	c := store.pool.Get()
	defer c.Close()

	_, err := c.Do("SETEX", key, expires, val)
	return err
}
