package fixtures

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

var RunningInsideContainer = isRunningInContainer()

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

	if f.Network, err = f.getOrCreateNetwork(); err != nil {
		return err
	}

	return nil
}

func (f *Docker) TearDown(context.Context) error {
	if RunningInsideContainer {
		// This is a total hack - if we call close on the network we'll get a seg fault because
		// the pool is nil.
		return nil
	}
	if err := f.Network.Close(); err != nil {
		return err
	}
	return nil
}

func (f *Docker) getOrCreateNetwork() (*dockertest.Network, error) {
	if !RunningInsideContainer {
		nw, err := f.Pool.CreateNetwork(f.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to create docker network: %w", err)
		}
		return nw, nil
	}

	hostNetworkName := os.Getenv("HOST_NETWORK_NAME")
	if hostNetworkName == "" {
		nw, err := f.Pool.CreateNetwork(f.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to create docker network: %w", err)
		}
		return nw, nil
	}

	ns, err := f.Pool.Client.FilteredListNetworks(map[string]map[string]bool{
		"name": {hostNetworkName: true},
	})
	if err != nil {
		return nil, fmt.Errorf("error listing docker networks: %w", err)
	}
	if len(ns) != 1 {
		return nil, fmt.Errorf("list networks returned %d results, expected only 1", len(ns))
	}
	// This struct also contains an unexported reference to pool.
	// Unfortunately, the framework doesn't give us a way to construct a Network struct in any way
	// other than calling the CreateNetwork function, so we just have to leave the pool unset.
	// The result is that we can't call Close() on this network without producing a seg fault.  However,
	// calling Close() also disconnects ALL the containers from the network, which isn't desirable
	// when running inside of a host container, because we don't actually want to disconncet the host
	// from the network.
	return &dockertest.Network{Network: &ns[0]}, nil
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

// GetContainerAddress returns the address at which requests from the tests can be made into the
// container.
// When running inside a host container and connected to a bridge network, this returns
// the address of the container as known by the container network.
// When running inside a host container and not connected to a bridge network, this returns the
// network gateway.
// When not running inside a host container it returns localhost.
func GetContainerAddress(resource *dockertest.Resource, network *dockertest.Network) string {
	if UseBridgeNetwork(network) {
		return GetHostIP(resource, network)
	}
	if RunningInsideContainer {
		gw := resource.Container.NetworkSettings.Gateway
		if gw != "" {
			return gw
		}
		if nw, ok := resource.Container.NetworkSettings.Networks[network.Network.Name]; ok {
			return nw.Gateway
		}
	}
	return "localhost"
}

// GetContainerTcpPort returns the port which can be used to connect into the container from the test.
// When connected to a bridge network, the container exposed port can be used.
// Otherwise, we'll need to use the mapped port.
func GetContainerTcpPort(resource *dockertest.Resource, network *dockertest.Network, port string) string {
	if UseBridgeNetwork(network) {
		return port
	}
	return resource.GetPort(fmt.Sprintf("%s/tcp", port))
}

func UseBridgeNetwork(network *dockertest.Network) bool {
	if RunningInsideContainer {
		// Check if there is a connected container that matches the hostname, which means the host
		// container is connected to the network
		hostname, err := os.Hostname()
		if err != nil {
			panic(fmt.Errorf("error retrieving hostname: %w", err))
		}
		for _, v := range network.Network.Containers {
			if v.Name == hostname {
				return true
			}
		}
	}
	return false
}

// IsRunningInContainer checks if the current executable is running inside a container
// This implementation is currently docker-specific and won't work on other container engines, such
// as podman.
// A more portable solution is probably more ideal.
func isRunningInContainer() bool {
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	} else {
		panic(fmt.Errorf("error detecting if running inside container: %w", err))
	}
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
