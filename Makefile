-include $(shell [ -e .build-harness ] || curl -sSL -o .build-harness "https://git.io/fjZtV"; echo .build-harness)

.PHONY: clean
clean:
	go clean -testcache ./...

.PHONY: test
test:
	@mkdir -p testdata/tmp
	set -o pipefail; DEBUG=true go test ./... -v || ( \
		([ ! -z "$$(docker container ls -aq)" ] && docker container stop $$(docker container ls -aq)) && \
		([ ! -z "$$(docker container ls -aq)" ] && docker container rm $$(docker container ls -aq)) && \
		docker network prune -f \
	)
	@rm -rf testdata/tmp
