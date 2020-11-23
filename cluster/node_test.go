package cluster

import (
	"testing"

	// "github.com/benbjohnson/clock"
	// "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"logbin.io/shackle/config"
	"logbin.io/shackle/test/mock"
)

func TestNode(t *testing.T) {
	var (
		node           *node
		err            error
		logger         = &mock.Logger{}
		svcHash        = &mock.ServiceHash{}
		svcPersistence = &mock.ServicePersistence{}
	)
	t.Run("New", func(t *testing.T) {
		node, err = NewNode(
			config.Host{
				ID:        1,
				KeyLength: 16,
			},
			logger,
			svcHash,
			svcPersistence,
			0,
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
