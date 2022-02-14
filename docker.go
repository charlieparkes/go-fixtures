package fixtures

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"go.uber.org/zap"
)

type DockerOpt func(*Docker)

func NewDocker(opts ...DockerOpt) *Docker {
	f := &Docker{}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func DockerName(name string) DockerOpt {
	return func(f *Docker) {
		f.name = name
	}
}

func DockerNamePrefix(namePrefix string) DockerOpt {
	return func(f *Docker) {
		f.namePrefix = namePrefix
	}
}

func DockerNetworkName(networkName string) DockerOpt {
	return func(f *Docker) {
		f.networkName = networkName
	}
}

type Docker struct {
	BaseFixture
	name           string
	namePrefix     string
	networkName    string
	networkExisted bool
	pool           *dockertest.Pool
	network        *dockertest.Network
}

func (f *Docker) GetName() string {
	return f.name
}

func (f *Docker) GetNamePrefix() string {
	return f.namePrefix
}

func (f *Docker) GetNetworkName() string {
	return f.networkName
}

func (f *Docker) GetPool() *dockertest.Pool {
	return f.pool
}

func (f *Docker) GetNetwork() *dockertest.Network {
	return f.network
}

func (f *Docker) SetUp(ctx context.Context) error {
	var err error
	if f.namePrefix == "" {
		if f.name != "" {
			f.namePrefix = f.name
		} else {
			f.namePrefix = "test"
		}
	}

	if f.name == "" {
		f.name = namesgenerator.GetRandomName(0)
		if f.namePrefix != "" {
			f.name = f.namePrefix + "_" + f.name
		}
	}

	if f.networkName == "" {
		f.networkName = f.name
	}

	if f.pool, err = dockertest.NewPool(""); err != nil {
		return err
	}

	if f.network, err = f.getOrCreateNetwork(); err != nil {
		return err
	}

	return nil
}

func (f *Docker) TearDown(context.Context) error {
	if !f.networkExisted {
		if err := f.GetNetwork().Close(); err != nil {
			return err
		}
	}
	return nil
}

func (f *Docker) getOrCreateNetwork() (*dockertest.Network, error) {
	ns, err := f.GetPool().Client.FilteredListNetworks(map[string]map[string]bool{
		"name": {f.GetNetworkName(): true},
	})
	if err != nil {
		return nil, fmt.Errorf("error listing docker networks: %w", err)
	}
	if len(ns) == 1 {
		f.networkExisted = true
		// This struct also contains an unexported reference to pool.
		// Unfortunately, the framework doesn't give us a way to construct a Network struct in any way
		// other than calling the CreateNetwork function, so we just have to leave the pool unset.
		// The result is that we can't call Close() on this network without producing a seg fault.  However,
		// calling Close() also disconnects ALL the containers from the network, which isn't desirable
		// when running inside of a host container, because we don't actually want to disconncet the host
		// from the network.
		return &dockertest.Network{Network: &ns[0]}, nil
	}

	nw, err := f.GetPool().CreateNetwork(f.name)
	if err != nil {
		return nil, fmt.Errorf("failed to create docker network: %w", err)
	}
	return nw, nil
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

// GetContainerAddress returns the address at which requests from the tests can be made into the container.
// When running inside a host container and connected to a bridge network, this returns the address of the container as known by the container network.
// When running inside a host container and not connected to a bridge network, this returns the network gateway.
// When not running inside a host container it returns localhost.
func GetContainerAddress(resource *dockertest.Resource, network *dockertest.Network) string {
	if UseBridgeNetwork(network) {
		return GetHostIP(resource, network)
	}
	if IsRunningInContainer() {
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
	return false
}

// IsRunningInContainer checks if the current executable is running inside a container.
// This implementation is currently docker-specific and won't work on other container engines, such as podman.
// A more portable solution is probably more ideal.
func IsRunningInContainer() bool {
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	} else {
		panic(fmt.Errorf("error detecting if running inside container: %w", err))
	}
}

func getLogs(log *zap.Logger, containerID string, pool *dockertest.Pool) string {
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
		log.Warn("failed to read logs", zap.Error(err))
	}
	return buf.String()
}

func purge(p *dockertest.Pool, r *dockertest.Resource) {
	p.Purge(r)
	wg.Done()
}
