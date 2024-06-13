package redibolt

import (
	"github.com/stretchr/testify/assert"
	bolt "go.etcd.io/bbolt"
	"os"
	"testing"
)

func TestHashes(t *testing.T) {
	rdb := NewDB(makeTestDB(t))

	hexists, err := rdb.HEXISTS("k1", "f1")
	assert.NoError(t, err)
	assert.False(t, hexists)

	err = rdb.HSET("k1", "f1", "v1")
	assert.NoError(t, err)

	hexists, err = rdb.HEXISTS("k1", "f1")
	assert.NoError(t, err)
	assert.True(t, hexists)

	val, err := rdb.HGET("k1", "f1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	kvMap, err := rdb.HGETALL("k1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", kvMap["f1"])

	err = rdb.HSET("k1", "f2", "v2")
	assert.NoError(t, err)

	keys, err := rdb.HKEYS("k1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"f1", "f2"}, keys)

	count, err := rdb.HLEN("k1")
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	err = rdb.HDEL("k1", "f1")
	assert.NoError(t, err)

	keys, err = rdb.HKEYS("k1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"f2"}, keys)

	count, err = rdb.HLEN("k1")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	err = rdb.DEL("k1")
	assert.NoError(t, err)

	keys, err = rdb.HKEYS("k1")
	assert.NoError(t, err)
	assert.Empty(t, keys)

	err = rdb.HMSET("k2", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})
	assert.NoError(t, err)

	keys, err = rdb.HKEYS("k2")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"f1", "f2", "f3"}, keys)

	vals, err := rdb.HMGET("k2", "f1", "f3")
	assert.NoError(t, err)
	assert.Equal(t, []string{"v1", "v3"}, vals)
}

func TestNonExistentHashes(t *testing.T) {
	rdb := NewDB(makeTestDB(t))

	_, err := rdb.HGET("nokey", "f1")
	assert.NoError(t, err)

	_, err = rdb.HGETALL("nokey")
	assert.NoError(t, err)

	_, err = rdb.HKEYS("nokey")
	assert.NoError(t, err)

	_, err = rdb.HLEN("nokey")
	assert.NoError(t, err)

	_, err = rdb.HMGET("nokey", "f1", "f2")
	assert.NoError(t, err)

	err = rdb.HDEL("nokey", "f1")
	assert.NoError(t, err)

	err = rdb.DEL("nokey")
	assert.NoError(t, err)
}

func TestNonExistentSets(t *testing.T) {
	rdb := NewDB(makeTestDB(t))

	count, err := rdb.SCARD("nokey")
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	err = rdb.SREM("nokey", "m1")
	assert.NoError(t, err)

	isMember, err := rdb.SISMEMBER("nokey", "m1")
	assert.NoError(t, err)
	assert.False(t, isMember)

	members, err := rdb.SMEMBERS("nokey")
	assert.NoError(t, err)
	assert.Empty(t, members)
}

func TestSets(t *testing.T) {
	rdb := NewDB(makeTestDB(t))

	err := rdb.SADD("s1", "m1")
	assert.NoError(t, err)

	count, err := rdb.SCARD("s1")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	isMember, err := rdb.SISMEMBER("s1", "m1")
	assert.NoError(t, err)
	assert.True(t, isMember)

	members, err := rdb.SMEMBERS("s1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1"}, members)

	err = rdb.SADD("s1", "m2")
	assert.NoError(t, err)

	count, err = rdb.SCARD("s1")
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	isMember, err = rdb.SISMEMBER("s1", "m2")
	assert.NoError(t, err)
	assert.True(t, isMember)

	members, err = rdb.SMEMBERS("s1")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1", "m2"}, members)

	err = rdb.SREM("s1", "m1")
	assert.NoError(t, err)

	count, err = rdb.SCARD("s1")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	isMember, err = rdb.SISMEMBER("s1", "m1")
	assert.NoError(t, err)
	assert.False(t, isMember)

	isMember, err = rdb.SISMEMBER("s1", "nonmember")
	assert.NoError(t, err)
	assert.False(t, isMember)

	err = rdb.SMOVE("s1", "s3", "m2")
	assert.NoError(t, err)
	count, err = rdb.SCARD("s1")
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	count, err = rdb.SCARD("s3")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
	isMember, err = rdb.SISMEMBER("s3", "m2")
	assert.NoError(t, err)
	assert.True(t, isMember)
}

func TestMultiSetOps(t *testing.T) {
	rdb := NewDB(makeTestDB(t))

	err := rdb.SADD("s1", "m1", "m2")
	assert.NoError(t, err)

	err = rdb.SADD("s2", "m2", "m3")
	assert.NoError(t, err)

	diffMembers, err := rdb.SDIFF("s1", "s2")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"m1"}, diffMembers)
}

func TestTransactions(t *testing.T) {
	rdb := NewDB(makeTestDB(t))

	err := rdb.MULTIUPDATE(func(tx Tx) error {
		err := tx.SADD("s1", "m1")
		assert.NoError(t, err)

		err = tx.HSET("k1", "f1", "v1")
		assert.NoError(t, err)
		return nil
	})
	assert.NoError(t, err)

	count, err := rdb.SCARD("s1")
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	err = rdb.MULTIREAD(func(tx ReadTx) error {
		isMember, err := tx.SISMEMBER("s1", "m1")
		assert.NoError(t, err)
		assert.True(t, isMember)

		val, err := tx.HGET("k1", "f1")
		assert.NoError(t, err)
		assert.Equal(t, "v1", val)

		return nil
	})
	assert.NoError(t, err)
}

func makeTestDB(t *testing.T) *bolt.DB {
	tmpDB, err := os.CreateTemp("", t.Name())
	t.Cleanup(func() { _ = os.Remove(tmpDB.Name()) })
	assert.NoError(t, err)
	db, err := bolt.Open(tmpDB.Name(), 0600, nil)
	assert.NoError(t, err)
	return db
}
