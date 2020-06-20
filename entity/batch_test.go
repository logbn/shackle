package entity

import (
	"crypto/sha1"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchPartitioned(t *testing.T) {
	b := Batch{
		BatchItem{0, []byte("0")},
		BatchItem{1, []byte("1")},
		BatchItem{2, []byte("2")},
		BatchItem{3, []byte("3")},
	}
	bp := b.Partitioned(4)
	require.Len(t, bp, 4)
	for _, b := range bp {
		assert.Len(t, b, 1)
	}
	bp = b.Partitioned(2)
	require.Len(t, bp, 2)
	for _, b := range bp {
		assert.Len(t, b, 2)
	}
}

func TestBatchFromJson(t *testing.T) {
	h := &mockHasher{}
	batch, err := BatchFromRequest([]byte(`["a", "b", "c", "d"]`), []byte("text/json"), nil, h)
	require.Nil(t, err)
	require.NotNil(t, batch)
	assert.Len(t, batch, 4)
	for i, item := range batch {
		assert.Equal(t, i, item.N)
		assert.Equal(t, 16, len(item.Hash))
	}
	batch, err = BatchFromRequest([]byte(`["a", "b", "c", "d"]`), []byte("text/html"), nil, h)
	require.NotNil(t, err)
	require.Nil(t, batch)
	batch, err = BatchFromJson([]byte(`["a", "b", "c", "d"`), nil, h)
	require.NotNil(t, err)
	require.Nil(t, batch)
	batch, err = BatchFromJson([]byte(`{"a":1, "b":2}`), nil, h)
	require.NotNil(t, err)
	require.Nil(t, batch)
}

func TestBatchResponseToJson(t *testing.T) {
	out := BatchResponseToJson([]int8{ITEM_OPEN, ITEM_EXISTS, ITEM_LOCKED, ITEM_BUSY, ITEM_ERROR})
	require.NotNil(t, out)
	assert.Equal(t, `[0,1,2,3,4]`, string(out))

	out = BatchResponseToJson([]int8{})
	require.NotNil(t, out)
	assert.Equal(t, `[]`, string(out))
}

type mockHasher struct{}

func (h *mockHasher) Hash(a, b []byte) []byte {
	sha := sha1.Sum(append(a, b...))
	return sha[:16]
}
