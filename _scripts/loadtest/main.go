package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"net/http"
	"math/rand"
	"sync"

	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	"github.com/tsenart/vegeta/lib"
)

const (
	ITEM_ERROR = 0
	ITEM_OPEN  = 1
	ITEM_LOCKED= 2
	ITEM_BUSY  = 3
	ITEM_EXISTS= 4
)

func main() {
	fs := flag.FlagSet{}
	var (
		url        = fs.String("u", "", "Endpoint")
		rps        = fs.Int("r", 1, "Requests per second")
		batchSize  = fs.Int("b", 0, "Batch Size")
		duration   = fs.Duration("t", 0, "Time")
		interval   = fs.Duration("intv", 10 * time.Second, "Time")
		keepAlive  = fs.Bool("k", false, "Keepalive")
		maxConn    = fs.Int("c", 10000, "Max open idle connections per target host")
		maxWorkers = fs.Int("w", 8, "Max workers")
		pctlocks   = fs.Int("pctlocks", 0, "Percent Locks (max 100)")
	)
	err := fs.Parse(os.Args[1:])
	if err != nil {
		return
	}

	fail := false
	if len(*url) < 1 {
		fail = true
		fmt.Printf("Invalid argument for url: %s\n", *url)
	}
	if *rps < 1 || *rps > 1e8 {
		fail = true
		fmt.Printf("Invalid argument for r: %d\n", *rps)
	}
	if *batchSize > 35536 {
		fail = true
		fmt.Printf("Invalid argument for batch size: %d\n", *batchSize)
	}
	if *pctlocks < 0 {
		*pctlocks = 0
	}
	if *pctlocks > 100 {
		*pctlocks = 100
	}
	if fail {
		return
	}
	if *duration == 0 {
		def := time.Duration(10 * time.Second)
		duration = &def
	}

	rate := vegeta.Rate{Freq: *rps, Per: time.Second}
	targeter := newTargeter(*batchSize, *pctlocks, vegeta.Target{
		Method: "POST",
		URL:    strings.Trim(*url, "/"),
		Header: http.Header{
			"shackle-client-app": []string{"loadtest"},
			"shackle-client-id": []string{"loadtest-1"},
			"content-type": []string{"application/json"},
		},
	})
	attacker := vegeta.NewAttacker(
		vegeta.KeepAlive(*keepAlive),
		vegeta.MaxConnections(*maxConn),
		vegeta.MaxWorkers(uint64(*maxWorkers)),
	)

	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	var ticker = time.NewTicker(*interval)
	var metricsMutex sync.RWMutex
	var stats = map[int]int{
		ITEM_ERROR : 0,
		ITEM_OPEN  : 0,
		ITEM_LOCKED: 0,
		ITEM_BUSY  : 0,
		ITEM_EXISTS: 0,
	}
	var total int
	var irate int
	var metrics vegeta.Metrics
	fmt.Printf("total,rate,req,success,failure,avg,p50,p95,p99,max,time\n")
	go func() {
		for {
			select {
			case <-stop:
				attacker.Stop()
				return
			case <-ticker.C:
				metricsMutex.Lock()
				success := stats[ITEM_LOCKED] + stats[ITEM_EXISTS]
				failure := stats[ITEM_ERROR]
				metrics.Close()
				fmt.Printf("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%v\n",
					total,
					irate / int(*interval / time.Second),
					int(metrics.Rate),
					success,
					failure,
					int(metrics.Latencies.Mean / time.Millisecond),
					int(metrics.Latencies.P50 / time.Millisecond),
					int(metrics.Latencies.P95 / time.Millisecond),
					int(metrics.Latencies.P99 / time.Millisecond),
					int(metrics.Latencies.Max / time.Millisecond),
					time.Now().Format(time.RFC3339),
				)
				irate = 0
				for k := range stats {
					stats[k] = 0
				}
				metrics = vegeta.Metrics{}
				metricsMutex.Unlock()
			}
		}
	}()

	var r []int
	for res := range attacker.Attack(targeter, rate, *duration, "asdf") {
		metricsMutex.Lock()
		metrics.Add(res)
		json.Unmarshal(res.Body, &r)
		for _, i := range r {
			stats[i]++
		}
		total += len(r)
		irate += len(r)
		metricsMutex.Unlock()
	}
	metrics.Close()

	metrics_json, _ := json.MarshalIndent(metrics, "", "\t")
	fmt.Printf("%s\n", metrics_json)
	stats_json, _ := json.MarshalIndent(stats, "", "\t")
	fmt.Printf("%s\n", stats_json)

	close(stop)
}

func newTargeter(batchSize, pctlocks int, target vegeta.Target) vegeta.Targeter {
	return func(tgt *vegeta.Target) error {
		if tgt == nil {
			return vegeta.ErrNilTarget
		}
		*tgt = target
		if rand.Intn(100) < pctlocks {
			tgt.URL = tgt.URL + "/lock"
		} else {
			tgt.URL = tgt.URL + "/commit"
		}
		batch := make([]string, batchSize)
		for i := range batch {
			batch[i] = uuid.New().String()
		}
		tgt.Body, _ = json.Marshal(batch)
		return nil
	}
}
