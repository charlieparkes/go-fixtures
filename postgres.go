package fixtures

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest/v3"
	"github.com/tovala/ezsqlx"
)

type Postgres struct {
	BaseFixture
	Pool     *DockerPool
	Network  *DockerNetwork
	Settings *ezsqlx.ConnectionSettings
	Resource *dockertest.Resource
	Version  string
}

func (f *Postgres) SetUp() error {
	if f.Version == "" {
		f.Version = "latest"
	}
	opts := dockertest.RunOptions{
		Repository: "postgres",
		Tag:        f.Version,
		Env: []string{
			"POSTGRES_USER=" + f.Settings.User,
			"POSTGRES_PASSWORD=" + f.Settings.Password,
			"POSTGRES_DB=" + f.Settings.Database,
		},
		Networks: []*dockertest.Network{
			f.Network.Network,
		},
	}
	resource, err := f.Pool.Pool.RunWithOptions(&opts)
	f.Resource = resource
	if err != nil {
		return err
	}

	// Tell docker to kill the container after an unreasonable amount of test time to prevent orphans.
	f.Resource.Expire(600)

	waitForPostgres(f.Pool.Pool, f.Resource, f.Settings)

	return nil
}

func (f *Postgres) TearDown() error {
	return f.Pool.Pool.Purge(f.Resource)
}

func (f *Postgres) GetConnection(dbname string) (*sqlx.DB, func() error) {
	settings := f.Settings.Copy()
	if dbname != "" {
		settings.Database = dbname
	}
	connections, tearDown := ezsqlx.InitConnections(map[string]*ezsqlx.ConnectionSettings{
		f.Settings.Database: settings,
	})
	return connections[f.Settings.Database], tearDown
}

func (f *Postgres) GetHostIP() string {
	return f.Resource.Container.NetworkSettings.Networks[f.Network.Network.Network.Name].IPAddress
}

func (f *Postgres) GetHostName() string {
	return f.Resource.Container.Name[1:]
}

func (f *Postgres) Psql(cmd []string, mounts []string) (int, error) {
	// We're going to connect over the docker network
	settings := f.Settings.Copy()
	settings.Host = f.GetHostIP()
	psql := &Psql{
		Pool:     f.Pool,
		Network:  f.Network,
		Settings: settings,
		Mounts:   mounts,
		Cmd:      cmd,
	}
	err := psql.SetUp()
	psql.TearDown()
	return psql.ExitCode, err
}

func (f *Postgres) CreateDatabase(name string) error {
	fmt.Printf("Create database %v on server %v .. ", name, f.GetHostName())
	// exitCode, err := f.Psql([]string{"psql", fmt.Sprintf("--command=CREATE DATABASE %v", name)}, []string{})
	exitCode, err := f.Psql([]string{"createdb", "--template=template0", name}, []string{})
	fmt.Printf("%v\n", getStatusSymbol(exitCode))
	return err
}

func (f *Postgres) CopyDatabase(source string, target string) error {
	fmt.Printf("Copy database %v to %v on server %v .. ", source, target, f.GetHostName())
	exitCode, err := f.Psql([]string{"createdb", fmt.Sprintf("--template=%v", source), target}, []string{})
	fmt.Printf("%v\n", getStatusSymbol(exitCode))
	return err
}

func (f *Postgres) DropDatabase(name string) error {
	fmt.Printf("Drop database %v on server %v .. ", name, f.GetHostName())
	exitCode, err := f.Psql([]string{"dropdb", name}, []string{})
	fmt.Printf("%v\n", getStatusSymbol(exitCode))
	return err
}

func (f *Postgres) LoadTestData(schemaName string, path string) error {
	if _, err := os.Stat(path); err == nil {
		testdataDir, _ := filepath.Abs(filepath.Join("testdata"))
		fmt.Printf("Load %v data into database %v on server %v .. ", schemaName, f.Settings.Database, f.GetHostName())
		// exitCode, err := f.Psql([]string{"./tmp/load.sh", schemaName}, []string{fmt.Sprintf("%v:/tmp", testdataDir)})
		exitCode, err := f.Psql([]string{"psql", fmt.Sprintf("--file=/tmp/%v.sql", schemaName)}, []string{fmt.Sprintf("%v:/tmp", testdataDir)})
		fmt.Printf("%v\n", getStatusSymbol(exitCode))
		if err != nil {
			log.Fatalf("Failed to run psql (load schema): %s", err)
			return err
		}
	}
	return nil
}

func waitForPostgres(pool *dockertest.Pool, resource *dockertest.Resource, settings *ezsqlx.ConnectionSettings) {
	if err := pool.Retry(func() error {
		var err error
		settings.Port = resource.GetPort("5432/tcp")
		db, err := settings.Open()
		defer db.Close()
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
}

type Psql struct {
	BaseFixture
	Pool     *DockerPool
	Network  *DockerNetwork
	Settings *ezsqlx.ConnectionSettings
	Resource *dockertest.Resource
	Version  string
	Mounts   []string
	Cmd      []string

	ExitCode int
}

func (f *Psql) SetUp() error {
	if f.Version == "" {
		f.Version = "latest"
	}
	var err error
	opts := dockertest.RunOptions{
		Repository: "governmentpaas/psql",
		Tag:        f.Version,
		Env: []string{
			"PGUSER=" + f.Settings.User,
			"PGPASSWORD=" + f.Settings.Password,
			"PGDATABASE=" + f.Settings.Database,
			"PGHOST=" + f.Settings.Host,
			"PGPORT=5432",
		},
		Mounts: f.Mounts,
		Networks: []*dockertest.Network{
			f.Network.Network,
		},
		Cmd: f.Cmd,
	}
	resource, err := f.Pool.Pool.RunWithOptions(&opts)
	f.Resource = resource
	if err != nil {
		return err
	}
	f.ExitCode = waitForContainer(f.Pool.Pool, f.Resource)
	containerName := f.Resource.Container.Name[1:]
	containerID := f.Resource.Container.ID[0:11]
	if f.ExitCode != 0 {
		// getLogs() needs some work
		fmt.Printf("psql (name: %v, id: %v) '%v', exit %v\n", containerName, containerID, f.Cmd, f.ExitCode)
		errMsg := fmt.Sprintf("exited with error (%v)", f.ExitCode)
		return errors.New(errMsg)
	}
	return err
}

func (f *Psql) TearDown() error {
	return f.Pool.Pool.Purge(f.Resource)
}

type PostgresDatabaseCopy struct {
	Postgres *Postgres

	Settings     *ezsqlx.ConnectionSettings
	DatabaseName string
}

func (f *PostgresDatabaseCopy) SetUp() error {
	// Copy the postgres settings and update them to point at the container's docker network IP and new database
	f.Settings = f.Postgres.Settings.Copy()

	// If no DatabaseName is provided, create a database copy.
	if f.DatabaseName == "" {
		f.DatabaseName = namesgenerator.GetRandomName(0)
		err := f.Postgres.CopyDatabase(f.Settings.Database, f.DatabaseName)
		if err != nil {
			log.Fatalf("Failed to copy database: %s", err)
			return err
		}
	}
	f.Settings.Database = f.DatabaseName
	return nil
}


func (f *PostgresDatabaseCopy) TearDown() error {
	return f.Postgres.DropDatabase(f.DatabaseName)
}

func (f *PostgresDatabaseCopy) GetConnection() (*sqlx.DB, func() error) {
	return f.Postgres.GetConnection(f.DatabaseName)
}

type PostgresSchema struct {
	Postgres *Postgres

	Settings     *ezsqlx.ConnectionSettings
	DatabaseName string
}

func (f *PostgresSchema) SetUp() error {
	// Copy the postgres settings and update them to point at the container's docker network IP and new database
	f.Settings = f.Postgres.Settings.Copy()

	// If no DatabaseName is provided, create a temp database.
	if f.DatabaseName == "" {
		f.DatabaseName = namesgenerator.GetRandomName(0)
		err := f.Postgres.CreateDatabase(f.DatabaseName)
		if err != nil {
			log.Fatalf("Failed to create database: %s", err)
			return err
		}
	}
	f.Settings.Database = f.DatabaseName

	// Load schema for this database if it exists.
	// Note: We will load the schema based on the name of the database on the original database connection settings.
	schemaName := f.Postgres.Settings.Database
	path := getTestDataPath(schemaName + ".sql")
	err := f.Postgres.LoadTestData(schemaName, path)
	if err != nil {
		log.Fatalf("Failed to load test data: %s", err)
		return err
	}

	return nil
}

func (f *PostgresSchema) TearDown() error {
	return f.Postgres.DropDatabase(f.DatabaseName)
}

func (f *PostgresSchema) GetConnection() (*sqlx.DB, func() error) {
	return f.Postgres.GetConnection(f.DatabaseName)
}

type PostgresWithSchema struct {
	BaseFixture
	Pool     *DockerPool
	Network  *DockerNetwork
	Settings *ezsqlx.ConnectionSettings
	Version  string

	Postgres *Postgres
	Schema   *PostgresSchema
}

func (f *PostgresWithSchema) SetUp() error {
	var err error

	f.Postgres = &Postgres{
		Pool:     f.Pool,
		Network:  f.Network,
		Settings: f.Settings,
		Version:  f.Version,
	}
	err = f.Postgres.SetUp()
	if err != nil {
		return err
	}

	f.Schema = &PostgresSchema{
		Postgres:     f.Postgres,
		DatabaseName: f.Settings.Database,
	}
	err = f.Schema.SetUp()

	return err
}

func (f *PostgresWithSchema) GetConnection() (*sqlx.DB, func() error) {
	return f.Schema.GetConnection()
}

func (f *PostgresWithSchema) TearDown() error {
	// Don't bother tearing down schema since it does psql teardown after each run
	// and we're about to destroy the whole database anyways.
	// f.PostgresSchema.TearDown()
	return f.Postgres.TearDown()
}