.PHONY: test run build load zip clean

test:
	go test ./...

run:
	docker compose up --build

build:
	go build ./cmd/gateway
	go build ./cmd/processor
	go build ./cmd/dashboard
	go build ./cmd/loadgen

load:
	go run ./cmd/loadgen --url http://localhost:8080/v1/incidents --rps 10000 --workers 256 --duration 60s

clean:
	rm -rf dist
