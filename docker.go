package fixtures

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type Docker struct {
	BaseFixture
	Name       string
	NamePrefix string
	Pool       *dockertest.Pool
	Network    *dockertest.Network
}

func (f *Docker) SetUp(ctx context.Context) error {
	var err error

	if f.NamePrefix == "" {
		if f.Name != "" {
			f.NamePrefix = f.Name
		} else {
			f.NamePrefix = "test"
		}
	}

	if f.Name == "" {
		f.Name = namesgenerator.GetRandomName(0)
		if f.NamePrefix != "" {
			f.Name = f.NamePrefix + "_" + f.Name
		}
	}

	if f.Pool, err = dockertest.NewPool(""); err != nil {
		return err
	}

	if f.Network, err = f.Pool.CreateNetwork(f.Name); err != nil {
		return fmt.Errorf("failed to create docker network: %w", err)
	}

	return nil
}

func (f *Docker) TearDown(context.Context) error {
	if err := f.Network.Close(); err != nil {
		return err
	}
	return nil
}

func WaitForContainer(pool *dockertest.Pool, resource *dockertest.Resource) (int, error) {
	exitCode, err := pool.Client.WaitContainer(resource.Container.ID)
	if err != nil {
		err = fmt.Errorf("unable to wait for container: %w", err)
	}
	return exitCode, err
}

func GetHostIP(resource *dockertest.Resource, network *dockertest.Network) string {
	if n, ok := resource.Container.NetworkSettings.Networks[network.Network.Name]; ok {
		return n.IPAddress
	}
	return ""
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

func purge(p *dockertest.Pool, r *dockertest.Resource) {
	p.Purge(r)
	wg.Done()
}
