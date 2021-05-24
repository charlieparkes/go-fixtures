package docker

import (
	"bytes"
	"log"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func WaitForContainer(pool *dockertest.Pool, resource *dockertest.Resource) int {
	exitCode, err := pool.Client.WaitContainer(resource.Container.ID)
	if err != nil {
		log.Fatalf("Unable to wait for container: %s", err)
	}
	return exitCode
}

func GetHostIP(resource *dockertest.Resource, network *dockertest.Network) string {
	return resource.Container.NetworkSettings.Networks[network.Network.Name].IPAddress
}

func GetHostName(resource *dockertest.Resource) string {
	return resource.Container.Name[1:]
}

func getLogs(containerID string, pool *dockertest.Pool) string {
	var buf bytes.Buffer
	logsOpts := docker.LogsOptions{
		Container:    containerID,
		OutputStream: &buf,
		Follow:       true,
		Stdout:       true,
		Stderr:       true,
		Timestamps:   true,
	}
	err := pool.Client.Logs(logsOpts)
	if err != nil {
		log.Printf("Failed to read logs %v", err)
	}
	return buf.String()
}
