package redibolt

import "github.com/boltdb/bolt"

type db struct {
	boltDB *bolt.DB
}

func NewDB(boltDB *bolt.DB) DB {
	return &db{boltDB}
}

func (db *db) MULTIUPDATE(f func(tx Tx) error) error {
	return db.boltDB.Update(func(tx *bolt.Tx) error {
		return f(NewTx(tx))
	})
}

func (db *db) MULTIREAD(f func(tx ReadTx) error) error {
	return db.boltDB.View(func(tx *bolt.Tx) error {
		return f(NewTx(tx))
	})
}

func (db *db) DEL(key ...string) (err error) {
	return db.boltDB.Update(func(tx *bolt.Tx) error {
		return NewTx(tx).DEL(key...)
	})
}

func (db *db) HDEL(key string, field string) (err error) {
	return db.boltDB.Update(func(tx *bolt.Tx) error {
		return NewTx(tx).HDEL(key, field)
	})
}

func (db *db) HEXISTS(key string, field string) (exists bool, err error) {
	return exists, db.boltDB.View(func(tx *bolt.Tx) error {
		exists, err = NewTx(tx).HEXISTS(key, field)
		return err
	})
}

func (db *db) HGET(key string, field string) (val string, err error) {
	return val, db.boltDB.View(func(tx *bolt.Tx) error {
		val, err = NewTx(tx).HGET(key, field)
		return err
	})
}

func (db *db) HGETALL(key string) (kvMap map[string]string, err error) {
	return kvMap, db.boltDB.View(func(tx *bolt.Tx) error {
		kvMap, err = NewTx(tx).HGETALL(key)
		return err
	})
}

func (db *db) HKEYS(key string) (keys []string, err error) {
	return keys, db.boltDB.View(func(tx *bolt.Tx) error {
		keys, err = NewTx(tx).HKEYS(key)
		return err
	})
}

func (db *db) HLEN(key string) (count int, err error) {
	return count, db.boltDB.View(func(tx *bolt.Tx) error {
		count, err = NewTx(tx).HLEN(key)
		return err
	})
}

func (db *db) HMGET(key string, fields ...string) (vals []string, err error) {
	return vals, db.boltDB.View(func(tx *bolt.Tx) error {
		vals, err = NewTx(tx).HMGET(key, fields...)
		return err
	})
}

func (db *db) HMSET(key string, fields map[string]string) (err error) {
	return db.boltDB.Update(func(tx *bolt.Tx) error {
		return NewTx(tx).HMSET(key, fields)
	})
}

func (db *db) HSET(key string, field string, value string) (err error) {
	return db.boltDB.Update(func(tx *bolt.Tx) error {
		return NewTx(tx).HSET(key, field, value)
	})
}

func (db *db) SADD(key string, member string) (err error) {
	return db.boltDB.Update(func(tx *bolt.Tx) error {
		return NewTx(tx).SADD(key, member)
	})
}

func (db *db) SCARD(key string) (count int, err error) {
	return count, db.boltDB.View(func(tx *bolt.Tx) error {
		count, err = NewTx(tx).SCARD(key)
		return err
	})
}

func (db *db) SISMEMBER(key string, member string) (isMember bool, err error) {
	return isMember, db.boltDB.View(func(tx *bolt.Tx) error {
		isMember, err = NewTx(tx).SISMEMBER(key, member)
		return err
	})
}

func (db *db) SMEMBERS(key string) (members []string, err error) {
	return members, db.boltDB.View(func(tx *bolt.Tx) error {
		members, err = NewTx(tx).SMEMBERS(key)
		return err
	})
}

func (db *db) SREM(key string, member string) (err error) {
	return db.boltDB.Update(func(tx *bolt.Tx) error {
		return NewTx(tx).SREM(key, member)
	})
}
