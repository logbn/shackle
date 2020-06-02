package main

import (
	"flag"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"highvolume.io/thunder/internal/app"
	"highvolume.io/thunder/internal/config"
	"highvolume.io/thunder/internal/log"
	"highvolume.io/thunder/internal/version"
)

var config_path = flag.String("c", "config.yml", "Location of config file")

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

	logger := log.NewLogrus()
	cfg := loadConfig(logger)
	log.SetLevelLogrus(logger, cfg.Log.Level)

	// App Startup
	var (
		api *app.Api
	)

	if cfg.Api.Enabled {
		api = app.NewApi(cfg, logger)
		logger.Infoln("Starting API")
		api.Start()
	}

	<-stop

	// App Shutdown
	if api != nil {
		logger.Infoln("Stopping API")
		api.Stop()
	}
	logger.Infoln("Done")
}

func loadConfig(logger log.Logger) config.App {
	cfg := config.App{}
	yamlFile, err := ioutil.ReadFile(*config_path)
	if err != nil {
		logger.Fatalf("Could not read config file %s, #%v ", *config_path, err)
	}
	err = yaml.Unmarshal(yamlFile, &cfg)
	if err != nil {
		logger.Fatalf("Could not unmarshal config file %s, #%v ", *config_path, err)
	}
	return cfg
}
