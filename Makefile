.PHONY: tidy generate check-style style test build run-tally run-gateway up down

tidy:
	go mod tidy

generate:
	buf generate

check-style:
	@output=$$(goimports -l $$(find . -name '*.go' -not -path './proto/*')); \
	if [ -n "$$output" ]; then echo "Files not formatted:"; echo "$$output"; exit 1; fi

style:
	goimports -l -w $(shell find . -name '*.go' -not -path './proto/*')

test:
	go test ./...

build:
	go build -o bin/tally ./cmd/tally
	go build -o bin/gateway ./cmd/gateway

run-tally:
	mkdir -p /tmp/tally-data
	go run ./cmd/tally --data-dir /tmp/tally-data --grpc-port 9090 --health-port 8081

run-gateway:
	go run ./cmd/gateway --tally-addr localhost:9090 --http-port 8080

up:
	docker compose up --build

down:
	docker compose down
