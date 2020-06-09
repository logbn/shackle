package log

import (
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
)

func TestLog(t *testing.T) {
	log := NewLogrus()
	assert.Equal(t, logrus.InfoLevel, log.GetLevel())
	err := SetLevelLogrus(log, "fatal")
	assert.Nil(t, err)
	assert.Equal(t, logrus.FatalLevel, log.GetLevel())
	err = SetLevelLogrus(log, "nonsense")
	assert.NotNil(t, err)
}
