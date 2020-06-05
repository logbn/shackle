package entity

import (
	"crypto/sha1"
	"sync"
	"strconv"

	"github.com/valyala/fastjson"
)

var batchParserPool = sync.Pool{New: func() interface{} { return new(fastjson.Parser) }}

// Batch represents an incoming batch of items to lock
type Batch [][]byte

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
		ent[i] = sha[:16]
	}
	return
}

func BatchResponseToJson(res []int) (out []byte, err error) {
	out = make([]byte, len(res)*2+1)
	out[0] = byte('[')
	for i, c := range res {
		out[2*i+1] = []byte(strconv.Itoa(c))[0]
		out[2*i+2] = byte(',')
	}
	out[len(res)*2] = byte(']')
	return
}
