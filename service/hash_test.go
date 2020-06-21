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
			Cluster: &config.Cluster{
				Pepper:    "pepper",
				KeyLength: 16,
			},
		})
		require.Nil(t, err)
		require.NotNil(t, svc)

		res := svc.Hash([]byte("test"), nil)
		assert.Equal(t, 16, len(res))
		assert.Equal(t, "4d0a121cdddc737f464d3d212056cfe5", hex.EncodeToString(res))
		// SHA1("peppertest")

		svc, err = NewHash(&config.App{
			Cluster: &config.Cluster{
				Pepper:    "pepper",
				KeyLength: 24,
			},
		})
		require.Nil(t, err)
		require.NotNil(t, svc)

		res = svc.Hash([]byte("test"), []byte("bucket"))
		assert.Equal(t, 24, len(res))
		assert.Equal(t, "46bc75c4efcf2a0a3902e51b79bd3db318965ee4cb0da2b4", hex.EncodeToString(res))
		// SHA256("pepperbuckettest")

		svc, err = NewHash(&config.App{
			Cluster: &config.Cluster{
				Pepper:    "pepper",
				KeyLength: 48,
			},
		})
		require.Nil(t, err)
		require.NotNil(t, svc)

		res = svc.Hash([]byte("test"), []byte("bucket2"))
		assert.Equal(t, 48, len(res))
		assert.Equal(t, "7f260afb8291ece77797c3a367de4015c4d45558bd79e46db18d17"+
			"82a27c2fdb1e76c391b053a7b29cd091546496284d", hex.EncodeToString(res))
		// SHA512("pepperbucket2test")
	})
	t.Run("Failure", func(t *testing.T) {
		svc, err := NewHash(&config.App{
			Cluster: &config.Cluster{
				Pepper:    "test",
				KeyLength: 80,
			},
		})
		require.NotNil(t, err)
		require.Nil(t, svc)
	})
}
