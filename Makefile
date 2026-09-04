.PHONY: test check contracts studio build e2e loadtest

test:
	go test ./...

check:
	go run ./cmd/generate-node-contract
	git diff --exit-code -- contracts/schemas/builtin-node-types-v1.json
	go test -p 1 -race ./...
	go vet ./...
	npm --prefix web test
	npm --prefix web run typecheck

contracts:
	go run ./cmd/generate-node-contract

studio:
	npm --prefix web ci
	npm --prefix web run build

build: contracts studio
	mkdir -p bin
	go build -o bin/control-plane ./cmd/control-plane
	go build -o bin/worker ./cmd/worker
	go build -o bin/migrate ./cmd/migrate
	go build -o bin/isolation ./cmd/isolation
	go build -o bin/ops ./cmd/ops
	go build -o bin/fenix-migrate ./cmd/fenix-migrate

e2e:
	go test -tags=e2e ./tests/e2e/...

loadtest:
	go test -run '^$$' -bench=. -benchmem ./tests/load/...
