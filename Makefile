.PHONY: tidy generate check-style style lint test build

tidy:
	go mod tidy

generate:
	buf generate

check-style:
	@output=$$(goimports -l $$(find . -name '*.go' -not -path './proto/*')); \
	if [ -n "$$output" ]; then echo "Files not formatted:"; echo "$$output"; exit 1; fi

style:
	goimports -l -w $(shell find . -name '*.go' -not -path './proto/*')

lint:
	staticcheck $(shell go list ./... | grep -v "$(shell go list -m)/proto")

test:
	go test ./...

build:
	go build -o bin/tally ./cmd/tally
