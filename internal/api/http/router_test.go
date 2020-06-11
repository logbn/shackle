package http

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"highvolume.io/shackle/internal/test/mock"
)

func TestRouter(t *testing.T) {
	l := &mock.Logger{}
	p := &mock.ServicePersistence{}
	r := NewRouter(l, p)
	assert.NotNil(t, r)
}
