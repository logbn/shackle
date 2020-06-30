VERSION := $(shell git describe --tags --always)
HASH := $(shell git rev-parse --short HEAD)
PROJECTNAME := shackle

LDFLAGS := -ldflags "-X 'main.Version=$(VERSION)' -X 'main.Hash=$(HASH)'"

DATA_DIR := /data

clean:
	@rm -rf _dist

clean-data:
	@rm -rf ${DATA_DIR}/{node,raft}*

build:
	@mkdir -p _dist
	go build $(LDFLAGS) -o _dist/$(PROJECTNAME) main.go

coverage:
	@mkdir -p _dist
	go test ./... -coverprofile=dist/coverage.out -coverpkg=./...
	go tool cover -html _dist/coverage.out -o _dist/coverage.html
	@go tool cover -func _dist/coverage.out | grep total | awk '{print $3}'

gen:
	protoc api/intapi/grpc.proto --go_out=plugins=grpc:. --go_opt=paths=source_relative

dev:
	@mkdir -p ${DATA_DIR}/node{1,2,3}
	@mkdir -p ${DATA_DIR}/raft{1,2,3}
	go run main.go -c _example/config.1.yml,_example/config.2.yml,_example/config.3.yml

.PHONY: help
all: help
help: Makefile
	@echo
	@echo " Choose a command run in "$(PROJECTNAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo
