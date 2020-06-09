package mock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogger(t *testing.T) {
	log := &Logger{}
	log.Info("a")
	log.Warn("a")
	log.Error("a")
	log.Debugf("b")
	log.Infof("b")
	log.Warnf("b")
	log.Errorf("b")
	assert.Len(t, log.Debugs, 1)
	assert.Len(t, log.Infos, 2)
	assert.Len(t, log.Warns, 2)
	assert.Len(t, log.Errors, 2)
	assert.Len(t, log.String(), 19)
	log.Reset()
	assert.Len(t, log.Debugs, 0)
	assert.Len(t, log.Infos, 0)
	assert.Len(t, log.Warns, 0)
	assert.Len(t, log.Errors, 0)
	assert.Len(t, log.String(), 3)
}
