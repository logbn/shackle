package service

import (
	"fmt"
	"math"

	"github.com/twmb/murmur3"

	"logbin.io/shackle/config"
)

type Hash interface {
	Hash([]byte, []byte) ([]byte, uint16)
	GetPartition([]byte) uint16
}

type hash struct {
	hashFunc func([]byte) []byte
	partMask uint16
}

// NewHash returns a hash service
func NewHash(cfg *config.App) (r *hash, err error) {
	var hashFunc func([]byte) []byte
	var partitions = cfg.Host.Partitions
	if partitions == 0 {
		return nil, fmt.Errorf("At least one partition is required (got %d)", partitions)
	}
	p1, p2 := murmur3.Sum128([]byte(cfg.Host.Pepper))
	hashFunc = func(in []byte) []byte {
		h1, h2 := murmur3.SeedSum128(p1, p2, in)
		return []byte{
			byte(h1>>56), byte(h1>>48), byte(h1>>40), byte(h1>>32),
			byte(h1>>24), byte(h1>>16), byte(h1>>8), byte(h1),
			byte(h2>>56), byte(h2>>48), byte(h2>>40), byte(h2>>32),
			byte(h2>>24), byte(h2>>16), byte(h2>>8), byte(h2),
		}
	}
	// Partition bitmask
	bits := int(math.Log2(float64(partitions)))
	mask := uint16(math.MaxUint16 >> (16 - bits) << (16 - bits))
	return &hash{hashFunc, mask}, nil
}

// Hash takes a byte array and returns a peppered hash
func (h *hash) Hash(item, bucket []byte) (out []byte, p uint16) {
	out = h.hashFunc(append(bucket, item...))
	p = h.GetPartition(out)
	return
}

// GetPartition returns just the partition
func (h *hash) GetPartition(item []byte) (p uint16) {
	p = binary.BigEndian.Uint16(item[:2]) & h.partMask
	return
}
