devsetup:
	go get -u "github.com/kisielk/errcheck"
	go get -u "golang.org/x/lint/golint"
	go get -u "github.com/gordonklaus/ineffassign"
	go get -u "github.com/client9/misspell/cmd/misspell"
	go get github.com/vektra/mockery/.../

test:
	go test ./...
fasttest:
	go test -short ./...

cover:
	go test -coverprofile=cover.out ./...

checkerrs:
	errcheck -blank -asserts -ignoretests ./...

checkfmt:
	! gofmt -l -d ./ 2>&1 | read

checkvet:
	go vet ./...

checkiea:
	ineffassign ./

checkspell:
	find . -type f -name '*.go' | grep -v vendor/ | xargs misspell -error

lint: checkfmt checkerrs checkvet checkiea checkspell
	golint -set_exit_status -min_confidence 0.81 ./...

check: lint
	go test -cover -race ./...

updatemocks:
	mockery -name dispatcher -output . -testonly -inpkg
	mockery -name tracerManager -output . -testonly -inpkg
	mockery -name configManager -output . -testonly -inpkg

.PHONY: all test devsetup fasttest lint cover checkerrs checkfmt checkvet checkiea checkspell check updatemocks
