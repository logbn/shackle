package entity

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/valyala/fastjson"
)

const (
	ITEM_ERROR  uint8 = 0 //  Retry after timeout     - Lock not granted
	ITEM_OPEN   uint8 = 1 //  Lock required           - Lock granted
	ITEM_LOCKED uint8 = 2 //  Proceed with processing - Lock granted
	ITEM_BUSY   uint8 = 3 //  Retry after timeout     - Lock not granted
	ITEM_EXISTS uint8 = 4 //  Do not proceed          - Lock not granted

	OP_LOCK     uint8 = 0
	OP_COMMIT   uint8 = 1
	OP_ROLLBACK uint8 = 2
)

type hasher interface {
	Hash(item, bucket []byte) (hash []byte, partition uint64)
}

var batchParserPool = sync.Pool{New: func() interface{} { return new(fastjson.Parser) }}

// Batch represents an incoming batch of items to lock
type Batch []BatchItem
type BatchItem struct {
	N         int
	Partition uint64
	Hash      []byte
}

// Partitioned splits a batch into a map of sub-batches keyed by partition prefix
func (b Batch) Partitioned() (batches map[uint64]Batch) {
	batches = map[uint64]Batch{}
	for i, item := range b {
		batches[item.Partition] = append(batches[item.Partition], item)
		batches[item.Partition][len(batches[item.Partition])-1].N = i
	}
	return
}

// PartitionIndexed splits a batch into a map of sub-batches keyed by partition index
func (b Batch) PartitionIndexed(partitionCount int) (batches map[int]Batch) {
	batches = map[int]Batch{}
	var bits = int(math.Log2(float64(partitionCount)))
	var partitionIndex int
	for i, item := range b {
		partitionIndex = int(item.Partition >> (64 - bits))
		batches[partitionIndex] = append(batches[partitionIndex], item)
		batches[partitionIndex][len(batches[partitionIndex])-1].N = i
	}
	return
}

// BatchPlan is a map of batches keyed by hostID
type BatchPlan map[uint64]*BatchPlanSegment
type BatchPlanSegment struct {
	NodeAddr string
	Batch    Batch
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
	var hash []byte
	var partition uint64
	for i, sv := range values {
		hash, partition = h.Hash(sv.GetStringBytes(), bucket)
		ent[i] = BatchItem{i, partition, hash}
	}
	return
}

// BatchResponseToJson converts a batch response (uint8 slice) to json
func BatchResponseToJson(res []uint8) (out []byte) {
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
