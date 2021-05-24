package docker

import (
	"log"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/ory/dockertest/v3"
)

type Docker struct {
	Name       string
	NamePrefix string

	Pool    *dockertest.Pool
	Network *dockertest.Network
}

func (f *Docker) SetUp() error {
	var err error

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
		log.Fatalf("Failed to create docker network: %v", err)
	}

	return nil
}

func (f *Docker) TearDown() error {
	if err := f.Network.Close(); err != nil {
		return err
	}
	return nil
}
