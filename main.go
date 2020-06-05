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

	"highvolume.io/shackle/internal/app"
	"highvolume.io/shackle/internal/config"
	"highvolume.io/shackle/internal/log"
	"highvolume.io/shackle/internal/version"
)

var config_path = flag.String("c", "config.yml", "Location of config file(s)")
var dev = flag.Bool("dev", false, "Whether to load default set of config files")

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
	if *dev {
		paths = []string{"config.1.yml", "config.2.yml", "config.3.yml"}
	}

	logger := log.NewLogrus()

	wg := sync.WaitGroup{}
	started := make(chan bool)
	shutdown := make(chan bool)
	for i := 0; i < len(paths); i++ {
		wg.Add(1)
		go func(i int) {
			cfg := loadConfig(logger, paths[i])
			log.SetLevelLogrus(logger, cfg.Log.Level)
			// App Startup
			var (
				api *app.Api
			)

			if cfg.Api.Enabled {
				api = app.NewApi(cfg, logger)
				logger.Infof("Starting API %d", i)
				api.Start()
			}

			started <- true

			<-shutdown

			// App Shutdown
			if api != nil {
				logger.Infof("Stopping API %d", i)
				api.Stop()
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
	return cfg
}
