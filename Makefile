VERSION := $(shell git describe --tags)
HASH := $(shell git rev-parse --short HEAD)
PROJECTNAME := $(shell basename "$(PWD)")

LDFLAGS := -ldflags "-X 'main.Version=$(VERSION)' -X 'main.Hash=$(HASH)'"

AZ := $(shell curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)
REGION := $(shell curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone | rev | cut -c 2- | rev)

AWS_DEFAULT_REGION := $(REGION)

clean:
	rm -rf ./dist/*

build:
	go build $(LDFLAGS) -o dist/$(PROJECTNAME) main.go

.PHONY: help
all: help
help: Makefile
	@echo
	@echo " Choose a command run in "$(PROJECTNAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo
