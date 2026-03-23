.PHONY: tidy generate style lint test build

tidy:
	go mod tidy

generate:
	buf generate

style:
	goimports -l -w ./

lint:
	staticcheck ./...

test:
	go test ./...

build:
	go build -o bin/tally ./cmd/tally
