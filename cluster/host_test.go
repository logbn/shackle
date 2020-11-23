package cluster

import (
	"testing"

	// "github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"logbin.io/shackle/config"
	"logbin.io/shackle/test/mock"
)

func TestHost(t *testing.T) {
	var (
		h              *host
		err            error
		logger         = &mock.Logger{}
		svcHash        = &mock.ServiceHash{}
		svcPersistence = &mock.ServicePersistence{}
		svcDelegation  = &mock.ServiceDelegation{}
	)
	t.Run("New", func(t *testing.T) {
		h, err = NewHost(
			config.Host{
				ID:           1,
				DeploymentID: 1,
				KeyLength:    16,
				RaftAddr:     "127.0.0.1:1001",
				RaftDir:      "/tmp",
			},
			logger,
			svcHash,
			svcPersistence,
			svcDelegation,
		)
		require.Nil(t, err)
		assert.NotNil(t, h)
	})
}
