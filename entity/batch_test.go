package entity

import (
	"crypto/sha1"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchPartitioned(t *testing.T) {
	b := Batch{
		BatchItem{0, []byte("0"), 0},
		BatchItem{1, []byte("1"), 1},
		BatchItem{2, []byte("2"), 2},
		BatchItem{3, []byte("3"), 3},
	}
	bp := b.Partitioned()
	require.Len(t, bp, 4)
	for _, b := range bp {
		assert.Len(t, b, 1)
		assert.Equal(t, []byte(fmt.Sprintf("%d", b[0].N)), b[0].Hash)
	}
	b = Batch{
		BatchItem{0, []byte("0"), 0},
		BatchItem{1, []byte("1"), 1},
		BatchItem{2, []byte("2"), 0},
		BatchItem{3, []byte("3"), 1},
	}
	bp = b.Partitioned()
	require.Len(t, bp, 2)
	for _, b := range bp {
		assert.Len(t, b, 2)
		assert.Equal(t, []byte(fmt.Sprintf("%d", b[0].N)), b[0].Hash)
	}
}

func TestBatchPartitionIndexed(t *testing.T) {
	b := Batch{
		BatchItem{0, []byte("0"), 0x0000},
		BatchItem{1, []byte("1"), 0x4000},
		BatchItem{2, []byte("2"), 0x8000},
		BatchItem{3, []byte("3"), 0xc000},
	}
	bp := b.PartitionIndexed(4)
	require.Equal(t, 4, len(bp), "%#v", bp)
	for _, b := range bp {
		assert.Len(t, b, 1)
		assert.Equal(t, []byte(fmt.Sprintf("%d", b[0].N)), b[0].Hash)
	}
	b = Batch{
		BatchItem{0, []byte("0"), 0x0000},
		BatchItem{1, []byte("1"), 0x8000},
		BatchItem{2, []byte("2"), 0x0000},
		BatchItem{3, []byte("3"), 0x8000},
	}
	bp = b.PartitionIndexed(2)
	require.Len(t, bp, 2)
	for _, b := range bp {
		assert.Len(t, b, 2)
		assert.Equal(t, []byte(fmt.Sprintf("%d", b[0].N)), b[0].Hash)
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
	codes := []uint8{ITEM_ERROR, ITEM_OPEN, ITEM_EXISTS, ITEM_LOCKED, ITEM_BUSY}
	out := BatchResponseToJson(codes)
	require.NotNil(t, out)
	expected := fmt.Sprintf(`[%d,%d,%d,%d,%d]`, codes[0], codes[1], codes[2], codes[3], codes[4])
	assert.Equal(t, expected, string(out))

	out = BatchResponseToJson([]uint8{})
	require.NotNil(t, out)
	assert.Equal(t, `[]`, string(out))
}

func TestBatchToBytes(t *testing.T) {
	h := &mockHasher{}
	b := Batch{
		BatchItem{0, []byte("000000000000"), 0},
		BatchItem{1, []byte("000000000001"), 0},
		BatchItem{2, []byte("000000000002"), 0},
		BatchItem{3, []byte("000000000003"), 0},
	}
	msg := b.ToBytes(OP_LOCK)
	assert.Len(t, msg, 1+4+(12*4))
	batch2, err := BatchFromBytes(msg[5:], 12, h)
	require.Nil(t, err)
	assert.Len(t, batch2, 4)
	ops, batches, err := MultiBatchFromBytes(append(msg, msg...), 12, h)
	require.Nil(t, err)
	assert.Len(t, ops, 2)
	assert.Len(t, batches, 2)
	assert.Len(t, batches[0], 4)
}

type mockHasher struct{}

func (h *mockHasher) Hash(a, b []byte) ([]byte, uint16) {
	sha := sha1.Sum(append(a, b...))
	return sha[:16], uint16(0)
}
func (h *mockHasher) GetPartition([]byte) uint16 {
	return uint16(0)
}
