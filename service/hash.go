package service

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"math"

	"highvolume.io/shackle/config"
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
	var (
		keylen     = cfg.Host.KeyLength
		pepper     = cfg.Host.Pepper
		partitions = cfg.Host.Partitions
	)
	if partitions == 0 {
		return nil, fmt.Errorf("At least one partition is required (got %d)", partitions)
	}
	if keylen <= 20 {
		hashFunc = func(in []byte) []byte {
			sha := sha1.Sum(append([]byte(pepper), in...))
			return sha[:keylen]
		}
	} else if keylen <= 32 {
		hashFunc = func(in []byte) []byte {
			sha := sha256.Sum256(append([]byte(pepper), in...))
			return sha[:keylen]
		}
	} else if keylen <= 64 {
		hashFunc = func(in []byte) []byte {
			sha := sha512.Sum512(append([]byte(pepper), in...))
			return sha[:keylen]
		}
	} else {
		return nil, fmt.Errorf("Key length too large %d (max 64)", keylen)
	}
	if cfg.Host.Partitions > math.MaxUint16 {
		return nil, fmt.Errorf("Cluster partition count too large %d (max %d)", keylen, math.MaxUint16)
	}
	var log2pc = math.Log2(float64(partitions))
	if math.Trunc(log2pc) != log2pc {
		return nil, fmt.Errorf("Cluster partition count must be power of 2")
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
