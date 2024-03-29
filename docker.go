package fixtures

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

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

func DockerLogger(logger *zap.Logger) DockerOpt {
	return func(f *Docker) {
		f.log = logger
	}
}

type Docker struct {
	BaseFixture
	log            *zap.Logger
	name           string
	namePrefix     string
	networkName    string
	networkExisted bool
	pool           *dockertest.Pool
	network        *dockertest.Network
}

func (f *Docker) Name() string {
	return f.name
}
func (f *Docker) NamePrefix() string {
	return f.namePrefix
}

func (f *Docker) NetworkName() string {
	return f.networkName
}

func (f *Docker) Pool() *dockertest.Pool {
	return f.pool
}

func (f *Docker) Network() *dockertest.Network {
	return f.network
}

func (f *Docker) SetUp(ctx context.Context) error {
	var err error
	if f.log == nil {
		f.log = logger()
	}
	if f.namePrefix == "" {
		if f.name != "" {
			f.namePrefix = f.name
		} else {
			f.namePrefix = "test"
		}
	}

	if f.name == "" {
		f.name = GetRandomName(0)
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

	if err := f.pool.Client.Ping(); err != nil {
		return fmt.Errorf("docker unavailable: %w", err)
	}

	if f.network, err = f.getOrCreateNetwork(); err != nil {
		return err
	}

	return nil
}

func (f *Docker) TearDown(context.Context) error {
	if !f.networkExisted {
		if err := f.Network().Close(); err != nil {
			return err
		}
	}
	return nil
}

func (f *Docker) getOrCreateNetwork() (*dockertest.Network, error) {
	ns, err := f.Pool().Client.FilteredListNetworks(map[string]map[string]bool{
		"name": {f.NetworkName(): true},
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

	nw, err := f.Pool().CreateNetwork(f.name)
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

func HostIP(resource *dockertest.Resource, network *dockertest.Network) string {
	if n, ok := resource.Container.NetworkSettings.Networks[network.Network.Name]; ok {
		return n.IPAddress
	}
	return ""
}

func HostName(resource *dockertest.Resource) string {
	return resource.Container.Name[1:]
}

// GetContainerAddress returns the address at which requests from the tests can be made into the container.
// When running inside a host container and connected to a bridge network, this returns the address of the container as known by the container network.
// When running inside a host container and not connected to a bridge network, this returns the network gateway.
// When not running inside a host container it returns localhost.
func ContainerAddress(resource *dockertest.Resource, network *dockertest.Network) string {
	if UseBridgeNetwork(network) {
		return HostIP(resource, network)
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
func ContainerTcpPort(resource *dockertest.Resource, network *dockertest.Network, port string) string {
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

func IsDockerRunning() bool {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return false
	}
	err = pool.Client.Ping()
	if err != nil {
		return false
	}
	return true
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

func (f *Docker) Purge(r *dockertest.Resource) {
	wg.Add(1)
	go func() {
		f.Pool().Purge(r)
		wg.Done()
	}()
}

// Deprecated: use Name()
func (f *Docker) GetName() string {
	return f.name
}

// Deprecated: use NamePrefix()
func (f *Docker) GetNamePrefix() string {
	return f.namePrefix
}

// Deprecated: use NetworkName()
func (f *Docker) GetNetworkName() string {
	return f.networkName
}

// Deprecated: use Pool()
func (f *Docker) GetPool() *dockertest.Pool {
	return f.pool
}

// Deprecated: use Network()
func (f *Docker) GetNetwork() *dockertest.Network {
	return f.network
}

// Deprecated: use HostIP()
func GetHostIP(resource *dockertest.Resource, network *dockertest.Network) string {
	return HostIP(resource, network)
}

// Deprecated: use HostName()
func GetHostName(resource *dockertest.Resource) string {
	return HostName(resource)
}

// Deprecated: use ContainerAddress()
func GetContainerAddress(resource *dockertest.Resource, network *dockertest.Network) string {
	return ContainerAddress(resource, network)
}

// Deprecated: use ContainerTcpPort()
func GetContainerTcpPort(resource *dockertest.Resource, network *dockertest.Network, port string) string {
	return ContainerTcpPort(resource, network, port)
}
