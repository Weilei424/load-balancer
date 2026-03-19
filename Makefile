BINARY := lb
BUILD_DIR := ./bin
PKG := github.com/Weilei424/load-balancer/cmd/lb

.PHONY: all build test race demo chaos loadtest bench clean fmt vet

all: build

build:
	mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY) $(PKG)

test:
	go test -timeout 60s ./...

race:
	go test -race -timeout 60s ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

demo: build
	@bash scripts/demo.sh

chaos: build
	@bash scripts/chaos.sh

loadtest: build
	@bash scripts/loadtest.sh

bench:
	go test -bench=. -benchmem -run=^$$ -count=3 ./...

clean:
	rm -rf $(BUILD_DIR) data/
