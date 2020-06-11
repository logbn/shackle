package entity

import (
	"crypto/sha1"
	"strconv"
	"sync"

	"github.com/valyala/fastjson"
)

const (
	ITEM_OPEN   int8 = 0 //  Lock required           - Lock granted
	ITEM_EXISTS int8 = 1 //  Do not proceed          - Lock not granted
	ITEM_LOCKED int8 = 2 //  Proceed with processing - Lock granted
	ITEM_BUSY   int8 = 3 //  Retry after timeout     - Lock not granted
	ITEM_ERROR  int8 = 4 //  Retry after timeout     - Lock not granted
)

var batchParserPool = sync.Pool{New: func() interface{} { return new(fastjson.Parser) }}

// Batch represents an incoming batch of items to lock
type Batch []BatchItem
type BatchItem struct {
	N    int
	Hash []byte
}

func (b Batch) Partitioned(n int) map[int]Batch {
	batches := make(map[int]Batch)
	for _, item := range b {
		// TODO - Get a better
		p := int(item.Hash[0]) % n
		if _, ok := batches[p]; !ok {
			batches[p] = Batch{}
		}
		batches[p] = append(batches[p], item)
	}
	return batches
}

func LockBatchFromRequest(body []byte) (ent Batch, err error) {
	ent, err = BatchFromJson(body)
	return
}

func BatchFromJson(body []byte) (ent Batch, err error) {
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

func BatchResponseToJson(res []int8) (out []byte) {
	if len(res) == 0 {
		return []byte("[]")
	}
	out = make([]byte, len(res)*2+1)
	out[0] = byte('[')
	for i, c := range res {
		out[2*i+1] = []byte(strconv.Itoa(int(c)))[0]
		out[2*i+2] = byte(',')
	}
	out[len(res)*2] = byte(']')
	return
}
