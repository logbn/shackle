package entity

import (
	"crypto/sha1"
	"strconv"
	"sync"

	"github.com/valyala/fastjson"
)

var batchParserPool = sync.Pool{New: func() interface{} { return new(fastjson.Parser) }}

// Batch represents an incoming batch of items to lock
type Batch []BatchItem
type BatchItem struct {
	N    int
	Hash []byte
}

const (
	LOCK_SUCCESS    int8 = 0 // Proceed             - Lock granted
	LOCK_EXISTS     int8 = 1 // Do not proceed      - Lock not granted
	LOCK_BUSY       int8 = 2 // Retry after timeout - Lock not granted
	LOCK_ABANDONNED int8 = 3 // State unknown       - Lock granted
	LOCK_ERROR      int8 = 4 // Pause processing    - Lock not granted
)

func LockBatchFromRequest(body []byte) (ent Batch, err error) {
	ent, err = LockBatchFromJson(body)
	return
}

func LockBatchFromJson(body []byte) (ent Batch, err error) {
	var p = batchParserPool.Get().(*fastjson.Parser)
	defer batchParserPool.Put(p)
	v, err := p.ParseBytes(body)
	if err != nil {
		return
	}
	values, err := v.Array()
	if err != nil {
		return
	}
	ent = make(Batch, len(values))
	for i, sv := range values {
		sha := sha1.Sum(sv.GetStringBytes())
		ent[i] = BatchItem{i, sha[:16]}
	}
	return
}

func BatchResponseToJson(res []int8) (out []byte, err error) {
	out = make([]byte, len(res)*2+1)
	out[0] = byte('[')
	for i, c := range res {
		out[2*i+1] = []byte(strconv.Itoa(int(c)))[0]
		out[2*i+2] = byte(',')
	}
	out[len(res)*2] = byte(']')
	return
}
