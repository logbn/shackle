package http

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"highvolume.io/shackle/test/mock"
	"highvolume.io/shackle/test/mock/clustermock"
)

func TestRouter(t *testing.T) {
	l := &mock.Logger{}
	p := &mock.ServicePersistence{}
	n := &clustermock.Node{p}
	h := &mock.ServiceHash{}
	r := NewRouter(l, n, h)
	assert.NotNil(t, r)
}