package cluster

import (
	"testing"

	// "github.com/benbjohnson/clock"
	// "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/test/mock"
)

func TestNode(t *testing.T) {
	var (
		node           *node
		err            error
		logger         = &mock.Logger{}
		svcHash        = &mock.ServiceHash{}
		svcPersistence = &mock.ServicePersistence{}
		svcDelegation  = &mock.ServiceDelegation{}
	)
	initChan := make(chan entity.Catalog)
	t.Run("New", func(t *testing.T) {
		node, err = NewNode(
			config.App{
				Host: &config.Host{
					ID:        1,
					KeyLength: 16,
				},
			},
			logger,
			svcHash,
			svcPersistence,
			svcDelegation,
			initChan,
		)
		require.Nil(t, err)
		require.NotNil(t, node)
	})
	t.Run("Start", func(t *testing.T) {
		node.Start()
	})
	t.Run("Stop", func(t *testing.T) {
		node.Stop()
	})
}
