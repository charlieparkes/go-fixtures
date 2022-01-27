-include $(shell [ -e .build-harness ] || curl -sSL -o .build-harness "https://git.io/fjZtV"; echo .build-harness)

.PHONY: clean
clean:
	go clean -testcache ./...

.PHONY: test
test:
	@mkdir -p testdata/tmp
	DEBUG=true go test ./... -v
	@rm -rf testdata/tmp