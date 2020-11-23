package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/lni/dragonboat/v3"
	dbclient "github.com/lni/dragonboat/v3/client"
	dbsm "github.com/lni/dragonboat/v3/statemachine"

	"logbin.io/shackle/config"
	"logbin.io/shackle/entity"
)

type Proposer interface {
	Propose(op uint8, batch entity.Batch) (res []uint8, err error)
	Stop()
}

type proposalProcessor func(body []byte, n int) error

type proposalBatcher struct {
	mutex     *sync.Mutex
	msgLimit  int
	waitTime  time.Duration
	batch     *proposalBatch
	clock     clock.Clock
	nodeHost  *dragonboat.NodeHost
	stopped   bool
	clusterID uint64
}

// NewProposerBatched returns a new batched proposer
func NewProposerBatched(cfg config.Batch, nodeHost *dragonboat.NodeHost, clusterID uint64) *proposalBatcher {
	return &proposalBatcher{
		clock:     clock.NewMock(),
		mutex:     &sync.Mutex{},
		msgLimit:  cfg.Size,
		waitTime:  cfg.Time,
		nodeHost:  nodeHost,
		clusterID: clusterID,
	}
}

func (b *proposalBatcher) newBatch() {
	var ba = newProposalBatch(b.nodeHost, b.clusterID)
	go func(ba *proposalBatch) {
		select {
		case <-time.After(b.waitTime):
			b.mutex.Lock()
			if ba.process() {
				b.batch = nil
			}
			b.mutex.Unlock()
		case <-ba.waiting:
		}
	}(ba)
	b.batch = ba
}

// Add adds an item to the current batch
func (b *proposalBatcher) Propose(op uint8, batch entity.Batch) (res []uint8, err error) {
	b.mutex.Lock()
	if b.stopped {
		err = fmt.Errorf("Batcher Stopped")
		return
	}
	if b.batch == nil {
		b.newBatch()
	}
	if b.batch.items+len(batch) > b.msgLimit {
		b.batch.process()
		b.newBatch()
	}
	var ba = b.batch
	var offset = ba.items
	ba.body = append(ba.body, batch.ToBytes(op)...)
	ba.items += len(batch)
	b.mutex.Unlock()
	<-ba.done
	err = ba.err
	if err != nil {
		return
	}
	if len(ba.resp.Data) < offset+len(batch) {
		err = fmt.Errorf("Batch response too small %d < %d", len(ba.resp.Data), offset+len(batch))
		return
	}
	res = ba.resp.Data[offset : offset+len(batch)]
	return
}

// Stop stops the batcher
func (b *proposalBatcher) Stop() {
	b.mutex.Lock()
	b.stopped = true
	var ba = b.batch
	if ba == nil {
		return
	}
	ba.process()
	b.mutex.Unlock()
	<-ba.done
}

type proposalBatch struct {
	items      int
	body       []byte
	waiting    chan struct{}
	done       chan struct{}
	mutex      *sync.Mutex
	processing bool
	nodeHost   *dragonboat.NodeHost
	sess       *dbclient.Session
	resp       dbsm.Result
	err        error
}

func newProposalBatch(nodeHost *dragonboat.NodeHost, clusterID uint64) *proposalBatch {
	var b = &proposalBatch{
		waiting:  make(chan struct{}),
		done:     make(chan struct{}),
		mutex:    &sync.Mutex{},
		nodeHost: nodeHost,
		sess:     nodeHost.GetNoOPSession(clusterID),
	}
	return b
}

func (b *proposalBatch) process() bool {
	b.mutex.Lock()
	if b.processing {
		return false
	}
	close(b.waiting)
	b.processing = true
	b.mutex.Unlock()
	go func() {
		ctx, _ := context.WithTimeout(context.Background(), raftTimeout)
		b.resp, b.err = b.nodeHost.SyncPropose(ctx, b.sess, b.body)
		close(b.done)
	}()
	return true
}
