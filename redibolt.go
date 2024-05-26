package redibolt

type RediboltTx interface {
	DEL(key ...string) (err error)
	HDEL(key string, field string) (err error)
	HEXISTS(key string, field string) (exists bool, err error)
	HGET(key string, field string) (val string, err error)
	HGETALL(key string) (kvMap map[string]string, err error)
	HKEYS(key string) (keys []string, err error)
	HLEN(key string) (count int, err error)
	HMGET(key string, fields ...string) (vals []string, err error)
	HMSET(key string, fields map[string]string) (err error)
	HSET(key string, field string, value string) (err error)

	SADD(key string, member string) (err error)
	SCARD(key string) (count int, err error)
	SISMEMBER(key string, member string) (isMember bool, err error)
	SMEMBERS(key string) (members []string, err error)
	SREM(key string, member string) (err error)
}

type RediboltDB interface {
	RediboltTx
}
