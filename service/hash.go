package service

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"

	"highvolume.io/shackle/config"
)

type Hash interface {
	Hash([]byte, []byte) []byte
}

type svcHash struct {
	hashFunc func([]byte) []byte
}

// NewHash returns a hash service
func NewHash(cfg *config.App) (r *svcHash, err error) {
	var hashFunc func([]byte) []byte
	var (
		keylen = cfg.Data.KeyLength
		pepper = []byte(cfg.Data.Pepper)
	)
	if keylen <= 20 {
		hashFunc = func(in []byte) []byte {
			sha := sha1.Sum(append(pepper, in...))
			return sha[:keylen]
		}
	} else if keylen <= 32 {
		hashFunc = func(in []byte) []byte {
			sha := sha256.Sum256(append(pepper, in...))
			return sha[:keylen]
		}
	} else if keylen <= 64 {
		hashFunc = func(in []byte) []byte {
			sha := sha512.Sum512(append(pepper, in...))
			return sha[:keylen]
		}
	} else {
		return nil, fmt.Errorf("Key length too large %d (max 64)", keylen)
	}
	return &svcHash{hashFunc}, nil
}

// Hash takes a byte array and returns a peppered hash
func (h *svcHash) Hash(item, bucket []byte) (out []byte) {
	return h.hashFunc(append(bucket, item...))
}