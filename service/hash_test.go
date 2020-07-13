package service

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"highvolume.io/shackle/config"
)

func TestHash(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		svc, err := NewHash(&config.App{
			Host: &config.Host{
				Pepper:     "pepper",
				KeyLength:  16,
				Partitions: 1,
			},
		})
		require.Nil(t, err)
		require.NotNil(t, svc)

		// SHA1("peppertest")
		res, _ := svc.Hash([]byte("test"), nil)
		assert.Equal(t, 16, len(res))
		assert.Equal(t, "4d0a121cdddc737f464d3d212056cfe5", hex.EncodeToString(res))

		svc, err = NewHash(&config.App{
			Host: &config.Host{
				Pepper:     "pepper",
				KeyLength:  24,
				Partitions: 1,
			},
		})
		require.Nil(t, err)
		require.NotNil(t, svc)

		// SHA256("pepperbuckettest")
		res, _ = svc.Hash([]byte("test"), []byte("bucket"))
		assert.Equal(t, 24, len(res))
		assert.Equal(t, "46bc75c4efcf2a0a3902e51b79bd3db318965ee4cb0da2b4", hex.EncodeToString(res))

		svc, err = NewHash(&config.App{
			Host: &config.Host{
				Pepper:     "pepper",
				KeyLength:  48,
				Partitions: 1,
			},
		})
		require.Nil(t, err)
		require.NotNil(t, svc)

		// SHA512("pepperbucket2test")
		res, _ = svc.Hash([]byte("test"), []byte("bucket2"))
		assert.Equal(t, 48, len(res))
		assert.Equal(t, "7f260afb8291ece77797c3a367de4015c4d45558bd79e46db18d17"+
			"82a27c2fdb1e76c391b053a7b29cd091546496284d", hex.EncodeToString(res))
	})
	t.Run("Failure", func(t *testing.T) {
		svc, err := NewHash(&config.App{
			Host: &config.Host{
				Pepper:     "test",
				KeyLength:  80,
				Partitions: 1,
			},
		})
		require.NotNil(t, err)
		require.Nil(t, svc)
	})
}

func BenchmarkHash(b *testing.B) {
	svc, err := NewHash(&config.App{
		Host: &config.Host{
			Pepper:     "pepper",
			KeyLength:  16,
			Partitions: 1,
		},
	})
	require.Nil(b, err)
	require.NotNil(b, svc)
	var input = []byte("464d3d212056cfe5")
	var bucket = []byte("bucket")
	var res = make([]byte, 16)
	var p uint16
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, p = svc.Hash(input, bucket)
	}
	_ = res
	_ = p
}
