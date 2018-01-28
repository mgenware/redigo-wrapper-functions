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

// GetValue returns the result of a GET command.
func (store *RedisWrapper) GetValue(key string) (interface{}, error) {
	c := store.pool.Get()
	defer c.Close()

	return c.Do("GET", key)
}

// GetStringValue returns the result of a GET command and converts it to string.
func (store *RedisWrapper) GetStringValue(key string) (string, error) {
	return redis.String(store.GetValue(key))
}

// GetIntValue returns the result of a GET command and converts it to int.
func (store *RedisWrapper) GetIntValue(key string) (string, error) {
	return redis.Int(store.GetValue(key))
}

// GetInt64Value returns the result of a GET command and converts it to int64.
func (store *RedisWrapper) GetInt64Value(key string) (string, error) {
	return redis.Int64(store.GetValue(key))
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

// GetValue returns the result of a GET command.
func (store *RedisWrapper) Do(keysAndArgs ...interface{}) (interface{}, error) {
	c := store.pool.Get()
	defer c.Close()

	return c.Do(keysAndArgs)
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
