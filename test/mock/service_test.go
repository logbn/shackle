package mock

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"highvolume.io/shackle/entity"
)

func TestServicePersistence(t *testing.T) {
	svc := &ServicePersistence{}
	batch := entity.Batch{
		entity.BatchItem{0, []byte("TEST")},
	}

	res, err := svc.Lock(batch)
	assert.Len(t, res, 1)
	assert.Nil(t, err)
	res, err = svc.Rollback(batch)
	assert.Len(t, res, 1)
	assert.Nil(t, err)
	res, err = svc.Commit(batch)
	assert.Len(t, res, 1)
	assert.Nil(t, err)
	svc.Start()
	svc.Stop()

	assert.Equal(t, 1, svc.Locks)
	assert.Equal(t, 1, svc.Rollbacks)
	assert.Equal(t, 1, svc.Commits)
	assert.Equal(t, 1, svc.Starts)
	assert.Equal(t, 1, svc.Stops)
}