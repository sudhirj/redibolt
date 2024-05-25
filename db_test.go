package redibolt

import (
	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
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

	err = rdb.HSET("k2", "f1", "v1")
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

	err = rdb.HDELALL("k1")
	assert.NoError(t, err)

	keys, err = rdb.HKEYS("k1")
	assert.NoError(t, err)
	assert.Empty(t, keys)
}

func makeTestDB(t *testing.T) *bolt.DB {
	tmpDB, err := os.CreateTemp("", t.Name())
	t.Cleanup(func() { _ = os.Remove(tmpDB.Name()) })
	assert.NoError(t, err)
	db, err := bolt.Open(tmpDB.Name(), 0600, nil)
	assert.NoError(t, err)
	return db
}
