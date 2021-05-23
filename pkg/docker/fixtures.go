package docker

import (
	"bytes"
	"log"

	"github.com/charlieparkes/go-fixtures/pkg/fixtures"
	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type DockerPool struct {
	fixtures.BaseFixture
	Pool *dockertest.Pool
}

func (f *DockerPool) SetUp() error {
	pool, err := dockertest.NewPool("")
	f.Pool = pool
	return err
}

func (f *DockerPool) TearDown() error {
	// don't do anything, since a pool is basically just a client in this context
	return nil
}

type DockerNetwork struct {
	fixtures.BaseFixture
	Pool       *DockerPool
	Network    *dockertest.Network
	Name       string
	NamePrefix string
}

func (f *DockerNetwork) SetUp() error {
	if f.Name == "" {
		f.Name = namesgenerator.GetRandomName(0)
		if f.NamePrefix != "" {
			f.Name = f.NamePrefix + "_" + f.Name
		}
	}
	network, err := f.Pool.Pool.CreateNetwork(f.Name)
	if err != nil {
		log.Fatalf("Failed to create docker network: %v", err)
	}
	f.Network = network
	return err
}

func (f *DockerNetwork) TearDown() error {
	return f.Network.Close()
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
	// err = f.Pool.Pool.Client.Logs(logsOpts)
	if err != nil {
		log.Printf("Failed to read logs %v", err)
	}
	return buf.String()
	// fmt.Printf("psql logs:\n%v", buf.String())
}
