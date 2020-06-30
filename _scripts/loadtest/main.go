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

	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	"github.com/tsenart/vegeta/lib"
)

func main() {
	fs := flag.FlagSet{}
	var (
		url        = fs.String("u", "", "Endpoint")
		rps        = fs.Int("r", 1, "Requests per second")
		batchSize  = fs.Int("b", 0, "Batch Size")
		duration   = fs.Duration("t", 0, "Time")
		keepAlive  = fs.Bool("k", false, "Keepalive")
		maxConn    = fs.Int("con", 10000, "Max open idle connections per target host")
		maxWorkers = fs.Int("w", 8, "Max workers")
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
	if fail {
		return
	}
	if *duration == 0 {
		def := time.Duration(10 * time.Second)
		duration = &def
	}

	rate := vegeta.Rate{Freq: *rps, Per: time.Second}
	targeter := newTargeter(*batchSize, vegeta.Target{
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
		vegeta.Connections(*maxConn),
		vegeta.MaxWorkers(uint64(*maxWorkers)),
	)

	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	go func() {
		select {
		case <-stop:
			attacker.Stop()
		}
	}()

	var metrics vegeta.Metrics
	for res := range attacker.Attack(targeter, rate, *duration, "asdf") {
		metrics.Add(res)
	}
	metrics.Close()

	metrics_json, _ := json.MarshalIndent(metrics, "", "\t")
	fmt.Printf("%s\n", metrics_json)

	close(stop)
}

func newTargeter(batchSize int, target vegeta.Target) vegeta.Targeter {
	return func(tgt *vegeta.Target) error {
		if tgt == nil {
			return vegeta.ErrNilTarget
		}
		*tgt = target
		batch := make([]string, batchSize)
		for i := range batch {
			batch[i] = uuid.New().String()
		}
		tgt.Body, _ = json.Marshal(batch)
		return nil
	}
}
