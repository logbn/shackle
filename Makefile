VERSION := $(shell git describe --tags)
HASH := $(shell git rev-parse --short HEAD)
PROJECTNAME := shackle

LDFLAGS := -ldflags "-X 'main.Version=$(VERSION)' -X 'main.Hash=$(HASH)'"

clean:
	rm -rf ./dist/*

build:
	go build $(LDFLAGS) -o dist/$(PROJECTNAME) main.go

coverage:
	go test ./... -coverprofile=dist/coverage.out -coverpkg=./...
	go tool cover -html=dist/coverage.out -o dist/coverage.html

.PHONY: help
all: help
help: Makefile
	@echo
	@echo " Choose a command run in "$(PROJECTNAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo
