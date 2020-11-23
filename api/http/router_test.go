package http

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"logbin.io/shackle/test/mock"
	"logbin.io/shackle/test/mock/mockcluster"
)

func TestRouter(t *testing.T) {
	l := &mock.Logger{}
	p := &mock.ServicePersistence{}
	n := &mockcluster.Host{SvcPersistence: p}
	h := &mock.ServiceHash{}
	r := NewRouter(l, n, h)
	assert.NotNil(t, r)
}
