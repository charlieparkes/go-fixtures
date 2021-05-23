package docker

import (
	"log"

	"github.com/ory/dockertest/v3"
)

func WaitForContainer(pool *dockertest.Pool, resource *dockertest.Resource) int {
	exitCode, err := pool.Client.WaitContainer(resource.Container.ID)
	if err != nil {
		log.Fatalf("Unable to wait for container: %s", err)
	}
	return exitCode
}
