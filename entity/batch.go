package entity

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/valyala/fastjson"
)

const (
	ITEM_OPEN   int8 = 0 //  Lock required           - Lock granted
	ITEM_EXISTS int8 = 1 //  Do not proceed          - Lock not granted
	ITEM_LOCKED int8 = 2 //  Proceed with processing - Lock granted
	ITEM_BUSY   int8 = 3 //  Retry after timeout     - Lock not granted
	ITEM_ERROR  int8 = 4 //  Retry after timeout     - Lock not granted

	CMD_LOCK     int8 = 0
	CMD_COMMIT   int8 = 1
	CMD_ROLLBACK int8 = 2
)

type hasher interface {
	Hash(item, bucket []byte) []byte
}

var batchParserPool = sync.Pool{New: func() interface{} { return new(fastjson.Parser) }}

// Batch represents an incoming batch of items to lock
type Batch []BatchItem
type BatchItem struct {
	N    int
	Hash []byte
}

// Partitioned splits a batch into a map of sub-batches keyed by partition number
func (b Batch) Partitioned(n int) map[int]Batch {
	batches := make(map[int]Batch)
	for _, item := range b {
		// TODO - Implement a bitmasked prefix partition strategy. mod(n) creates poorly ordered partitions.
		// ex. When using a prefix, splitting a partition requires scanning only the first half the shard.
		p := int(item.Hash[0]) % n
		if _, ok := batches[p]; !ok {
			batches[p] = Batch{}
		}
		batches[p] = append(batches[p], item)
	}
	return batches
}

// BatchFromRequest returns a batch given details about an http request
func BatchFromRequest(body, contentType []byte, bucket []byte, h hasher) (ent Batch, err error) {
	switch string(contentType) {
	case "text/json":
		fallthrough
	case "application/json":
		ent, err = BatchFromJson(body, bucket, h)
	default:
		err = fmt.Errorf("Invalid: Unrecognized content type %s", contentType)
	}

	return
}

// BatchFromJson returns a batch from a json byte slice
func BatchFromJson(body, bucket []byte, h hasher) (ent Batch, err error) {
	var p = batchParserPool.Get().(*fastjson.Parser)
	defer batchParserPool.Put(p)
	v, err := p.ParseBytes(body)
	if err != nil {
		err = fmt.Errorf("Invalid: %s", err.Error())
		return
	}
	values, err := v.Array()
	if err != nil {
		err = fmt.Errorf("Invalid: %s", err.Error())
		return
	}
	ent = make(Batch, len(values))
	for i, sv := range values {
		ent[i] = BatchItem{i, h.Hash(sv.GetStringBytes(), bucket)}
	}
	return
}

// BatchResponseToJson converts a batch response (int8 slice) to json
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

// IsValidation determines whether an error is a validation error
func IsValidation(err error) bool {
	return strings.HasPrefix(err.Error(), "Invalid: ")
}

// ErrorToJson marshals an error to standard json error response format
func ErrorToJson(err error) string {
	out, _ := json.Marshal(map[string]string{
		"error": err.Error(),
	})
	return string(out)
}
