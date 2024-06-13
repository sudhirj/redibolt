package redibolt

import (
	bolt "go.etcd.io/bbolt"
	"golang.org/x/exp/maps"
)

func NewTx(tx *bolt.Tx) Tx {
	return &rtx{tx}
}

type rtx struct {
	boltTx *bolt.Tx
}

func (t *rtx) DEL(key ...string) (err error) {
	for _, k := range key {
		_ = t.boltTx.DeleteBucket([]byte(k))
	}
	return
}

func (t *rtx) HDEL(key string, field string) (err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	return t.boltTx.Bucket([]byte(key)).Delete([]byte(field))
}

func (t *rtx) HEXISTS(key string, field string) (exists bool, err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	return b.Get([]byte(field)) != nil, nil
}

func (t *rtx) HGET(key string, field string) (val string, err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	return string(b.Get([]byte(field))), nil
}

func (t *rtx) HGETALL(key string) (kvMap map[string]string, err error) {
	kvMap = make(map[string]string)
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	_ = b.ForEach(func(k, v []byte) error {
		kvMap[string(k)] = string(v)
		return nil
	})
	return
}

func (t *rtx) HKEYS(key string) (keys []string, err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	err = b.ForEach(func(k, v []byte) error {
		keys = append(keys, string(k))
		return nil
	})
	return
}

func (t *rtx) HLEN(key string) (count int, err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	return b.Stats().KeyN, nil
}

func (t *rtx) HMGET(key string, fields ...string) (vals []string, err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	for _, field := range fields {
		vals = append(vals, string(b.Get([]byte(field))))
	}
	return
}

func (t *rtx) HMSET(key string, fields map[string]string) (err error) {
	b, err := t.boltTx.CreateBucketIfNotExists([]byte(key))
	if err != nil {
		return err
	}
	for k, v := range fields {
		if err = b.Put([]byte(k), []byte(v)); err != nil {
			return
		}
	}
	return
}

func (t *rtx) HSET(key string, field string, value string) (err error) {
	b, err := t.boltTx.CreateBucketIfNotExists([]byte(key))
	if err != nil {
		return err
	}
	return b.Put([]byte(field), []byte(value))
}

func (t *rtx) SADD(key string, member ...string) (err error) {
	for _, m := range member {
		if err = t.HSET(key, m, ""); err != nil {
			return
		}
	}
	return
}

func (t *rtx) SCARD(key string) (count int, err error) {
	return t.HLEN(key)
}

func (t *rtx) SISMEMBER(key string, member string) (isMember bool, err error) {
	return t.HEXISTS(key, member)
}

func (t *rtx) SMEMBERS(key string) (members []string, err error) {
	return t.HKEYS(key)
}

func (t *rtx) SREM(key string, member ...string) (err error) {
	for _, m := range member {
		if err = t.HDEL(key, m); err != nil {
			return
		}
	}
	return
}

func (t *rtx) SDIFF(key string, diffKeys ...string) (members []string, err error) {
	runner := make(map[string]struct{})
	members, err = t.SMEMBERS(key)
	if err != nil {
		return
	}

	for _, k := range members {
		runner[k] = struct{}{}
	}

	for _, k := range diffKeys {
		diffMembers, err := t.SMEMBERS(k)
		if err != nil {
			return nil, err
		}
		for _, m := range diffMembers {
			delete(runner, m)
		}
	}
	return maps.Keys(runner), nil
}

func (t *rtx) SMOVE(source string, destination string, member string) error {
	if isMember, err := t.SISMEMBER(source, member); err != nil || !isMember {
		return err
	}
	if err := t.SADD(destination, member); err != nil {
		return err
	}
	return t.SREM(source, member)
}
