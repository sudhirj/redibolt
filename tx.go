package redibolt

import "github.com/boltdb/bolt"

func NewTx(tx *bolt.Tx) *Tx {
	return &Tx{tx}
}

type Tx struct {
	boltTx *bolt.Tx
}

func (t *Tx) BoltTx() *bolt.Tx {
	return t.boltTx
}

func (t *Tx) HDEL(key string, field string) (err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	return t.boltTx.Bucket([]byte(key)).Delete([]byte(field))
}

func (t *Tx) HDELALL(key string) (err error) {
	return t.boltTx.DeleteBucket([]byte(key))
}

func (t *Tx) HEXISTS(key string, field string) (exists bool, err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	return b.Get([]byte(field)) != nil, nil
}

func (t *Tx) HGET(key string, field string) (val string, err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	return string(b.Get([]byte(field))), nil
}

func (t *Tx) HGETALL(key string) (kvMap map[string]string, err error) {
	kvMap = make(map[string]string)
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	err = b.ForEach(func(k, v []byte) error {
		kvMap[string(k)] = string(v)
		return nil
	})
	return kvMap, nil
}

func (t *Tx) HKEYS(key string) (keys []string, err error) {
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

func (t *Tx) HLEN(key string) (count int, err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	return b.Stats().KeyN, nil
}

func (t *Tx) HMGET(key string, fields ...string) (vals []string, err error) {
	b := t.boltTx.Bucket([]byte(key))
	if b == nil {
		return
	}
	for _, field := range fields {
		vals = append(vals, string(b.Get([]byte(field))))
	}
	return
}

func (t *Tx) HMSET(key string, fields map[string]string) (err error) {
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

func (t *Tx) HSET(key string, field string, value string) (err error) {
	b, err := t.boltTx.CreateBucketIfNotExists([]byte(key))
	if err != nil {
		return err
	}
	return b.Put([]byte(field), []byte(value))
}

func (t *Tx) SADD(key string, member string) (err error) {
	return t.HSET(key, member, "")
}

func (t *Tx) SCARD(key string) (count int, err error) {
	return t.HLEN(key)
}

func (t *Tx) SISMEMBER(key string, member string) (isMember bool, err error) {
	return t.HEXISTS(key, member)
}

func (t *Tx) SMEMBERS(key string) (members []string, err error) {
	return t.HKEYS(key)
}

func (t *Tx) SREM(key string, member string) (err error) {
	return t.HDEL(key, member)
}
