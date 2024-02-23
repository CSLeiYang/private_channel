.PHONY: default build

default: build

build:
	gofmt -w .
	go mod tidy
	go build -o private_channel_service .