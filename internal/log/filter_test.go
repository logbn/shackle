package log

import (
	"fmt"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingBuffer struct {
	b bytes.Buffer
}

func (b *failingBuffer) Write(in []byte) (res int, err error) {
	if string(in) == "error" {
		return 0, fmt.Errorf("error")
	}
	b.b.Write(in)
	res = len(in)
	return
}

func TestFilter(t *testing.T) {
	buf := &failingBuffer{bytes.Buffer{}}
	f := Filter{W: buf, Filters:[][]byte{[]byte("test1"), []byte("test2")}}
	res, err := f.Write([]byte("foo"))
	require.Nil(t, err)
	assert.Equal(t, res, 3)
	assert.Equal(t, "foo", string(buf.b.Bytes()))
	res, err = f.Write([]byte("footest1"))
	require.Nil(t, err)
	assert.Equal(t, res, 8)
	res, err = f.Write([]byte("test1bar"))
	require.Nil(t, err)
	assert.Equal(t, res, 8)
	res, err = f.Write([]byte("footest2bar"))
	require.Nil(t, err)
	assert.Equal(t, res, 11)
	res, err = f.Write([]byte("error"))
	require.NotNil(t, err)
}
