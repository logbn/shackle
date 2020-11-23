package mock

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"logbin.io/shackle/entity"
)

func TestServicePersistence(t *testing.T) {
	svc := &ServicePersistence{}
	batch := entity.Batch{
		entity.BatchItem{0, []byte("TEST"), 0},
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

	assert.Equal(t, 1, svc.metrics["Lock"])
	assert.Equal(t, 1, svc.metrics["Rollback"])
	assert.Equal(t, 1, svc.metrics["Commit"])
	assert.Equal(t, 1, svc.metrics["Start"])
	assert.Equal(t, 1, svc.metrics["Stop"])
}
