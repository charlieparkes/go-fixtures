-include $(shell [ -e .build-harness ] || curl -sSL -o .build-harness "https://git.io/fjZtV"; echo .build-harness)

.PHONY: test
test:
	go clean -testcache ./...
	@mkdir -p testdata/tmp
	DEBUG=true go test ./... -v
	@rm -rf testdata/tmp