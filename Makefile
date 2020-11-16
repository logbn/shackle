VERSION := $(shell git describe --tags --always)
HASH := $(shell git rev-parse --short HEAD)
PROJECTNAME := shackle

BUILD_TAGS := -tags=''

LDFLAGS := -ldflags "-X 'main.Version=$(VERSION)' -X 'main.Hash=$(HASH)'" $(BUILD_TAGS)

DATA_DIR := /data

DEV_CFG := _example/local_3x2/config.1.yml,_example/local_3x2/config.2.yml,_example/local_3x2/config.3.yml


clean:
	@rm -rf _dist

clean-data:
	@rm -rf $(DATA_DIR)/{node,raft}*

build:
	@mkdir -p _dist
	@go build $(LDFLAGS) -o _dist/$(PROJECTNAME) main.go

runtest:
	@go test ./... $(LDFLAGS)

coverage:
	@mkdir -p _dist
	go test ./... -coverprofile=_dist/coverage.out -coverpkg=./...
	go tool cover -html _dist/coverage.out -o _dist/coverage.html
	@go tool cover -func _dist/coverage.out | grep total | awk '{print $3}'

gen:
	protoc api/grpcint/grpc.proto --go_out=plugins=grpc:. --go_opt=paths=source_relative
	protoc entity/event/event.proto --go_out=plugins=grpc:. --go_opt=paths=source_relative

dev:
	@mkdir -p $(DATA_DIR)/node{1,2,3}
	@mkdir -p $(DATA_DIR)/raft{1,2,3}
	@go run $(LDFLAGS) main.go -c $(DEV_CFG)

tune: guard-DISK
	cat /proc/sys/vm/dirty_expire_centisecs
	@echo 100 | sudo tee /proc/sys/vm/dirty_expire_centisecs
	cat /proc/sys/vm/dirty_writeback_centisecs
	@echo 100 | sudo tee /proc/sys/vm/dirty_writeback_centisecs
	cat /proc/sys/vm/dirty_background_ratio
	@echo 50 | sudo tee /proc/sys/vm/dirty_background_ratio
	cat /proc/sys/vm/dirty_ratio
	@echo 80 | sudo tee /proc/sys/vm/dirty_ratio

guard-%:
	@ if [ "${${*}}" = "" ]; then \
		echo "Environment variable $* not set"; \
		exit 1; \
	fi

.PHONY: help
all: help
help: Makefile
	@echo
	@echo " Choose a command run in "$(PROJECTNAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo
