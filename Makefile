devsetup:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.55.2
	go install github.com/vektra/mockery/v2@v2.38.0

test:
	go test ./...
fasttest:
	go test -short ./...

cover:
	go test -coverprofile=cover.out ./...

lint:
	golangci-lint run -v

check: lint
	go test -cover -race ./...

updatemocks:
	mockery -name dispatcher -output . -testonly -inpkg
	mockery -name tracerManager -output . -testonly -inpkg
	mockery -name configManager -output . -testonly -inpkg
	mockery -name httpComponentInterface -output . -testonly -inpkg

.PHONY: all test devsetup fasttest lint cover checkerrs checkfmt checkvet checkiea checkspell check updatemocks
