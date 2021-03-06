package main

import (
	"flag"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"logbin.io/shackle/app"
	"logbin.io/shackle/config"
	"logbin.io/shackle/log"
	"logbin.io/shackle/version"
)

var config_path = flag.String("c", "config.yml", "Location of config file(s)")

var Version string
var Hash string

func main() {
	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	flag.Parse()

	if len(Version) > 0 {
		version.Number = Version
		version.Hash = Hash
	}

	rand.Seed(time.Now().UnixNano())

	paths := strings.Split(*config_path, ",")

	logger := log.NewLogrus()

	wg := sync.WaitGroup{}
	started := make(chan bool)
	shutdown := make(chan bool)
	for i := 0; i < len(paths); i++ {
		wg.Add(1)
		go func(i int) {
			cfg := loadConfig(logger, paths[i])
			err := log.SetLevelLogrus(logger, cfg.Log.Level)
			if err != nil {
				logger.Fatalf(err.Error())
			}

			if cfg.Host == nil {
				logger.Fatalf("Host misconfigured")
			}

			// App Startup
			var (
				cluster *app.Cluster
			)

			cluster, err = app.NewCluster(cfg, logger)
			if err != nil {
				logger.Fatalf("Error Starting Host %d - %s", cfg.Host.ID, err.Error())
			}
			logger.Infof("Starting Host %d", cfg.Host.ID)
			err = cluster.Start()
			if err != nil {
				logger.Fatalf("Error Starting Host %d - %s", cfg.Host.ID, err.Error())
			}

			started <- true

			<-shutdown

			// App Shutdown
			if cluster != nil {
				logger.Infof("Stopping Host %d", cfg.Host.ID)
				cluster.Stop()
			}
			logger.Infoln("Done")
			wg.Done()
		}(i)
		<-started
	}
	<-stop
	close(shutdown)
	wg.Wait()
}

func loadConfig(logger log.Logger, path string) config.App {
	cfg := config.App{}
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Fatalf("Could not read config file %s, #%v ", *config_path, err)
	}
	err = yaml.Unmarshal(yamlFile, &cfg)
	if err != nil {
		logger.Fatalf("Could not unmarshal config file %s, #%v ", *config_path, err)
	}
	cfg.SetDefaults()

	return cfg
}
