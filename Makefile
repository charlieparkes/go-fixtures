-include $(shell [ -e .build-harness ] || curl -sSL -o .build-harness "https://git.io/fjZtV"; echo .build-harness)

.DEFAULT_GOAL = all
all: fmt vet test

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: recover
recover:
	@set -o pipefail; ( \
		([ ! -z "$$(docker container ls -aq)" ] && docker container stop $$(docker container ls -aq)) && \
		([ ! -z "$$(docker container ls -aq)" ] && docker container rm $$(docker container ls -aq)) && \
		docker network prune -f && \
		exit 1 \
	)

.PHONY: test
test:
	@mkdir -p testdata/tmp
	set -o pipefail; DEBUG=true go test ./... -v || $(MAKE) recover
	@rm -rf testdata/tmp

.PHONY: test-docker
test-docker:
	@mkdir -p testdata/tmp
	set -o pipefail; \
		docker run --rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v $$PWD:$$PWD \
		--workdir $$PWD \
		-e DEBUG=true \
		golang:1.18 \
		go test ./... -v || $(MAKE) recover
	@rm -rf testdata/tmp

.PHONY: test-docker-network
test-docker-network:
	@mkdir -p testdata/tmp
	docker network create bridge-fixtures
	set -o pipefail; \
		docker run --rm \
		--network=bridge-fixtures \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v $$PWD:$$PWD \
		--workdir $$PWD \
		-e DEBUG=true \
		-e HOST_NETWORK_NAME=bridge-fixtures \
		golang:1.18 \
		go test ./... -v || $(MAKE) recover 
	@rm -rf testdata/tmp
