package fixtures

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/charlieparkes/ezsqlx"
	"github.com/charlieparkes/go-fixtures/internal/env"
	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest/v3"
)

const DEFAULT_POSTGRES_VERSION = "13-alpine"

type Postgres struct {
	BaseFixture
	Docker   *Docker
	Settings *ezsqlx.ConnectionSettings
	Resource *dockertest.Resource
	Version  string
	Expire   uint
}

func (f *Postgres) SetUp() error {
	if f.Version == "" {
		f.Version = DEFAULT_POSTGRES_VERSION
	}
	if f.Settings == nil {
		f.Settings = &ezsqlx.ConnectionSettings{
			Host:       "localhost",
			User:       "postgres",
			Password:   GenerateString(),
			Database:   f.Docker.Name,
			DisableSSL: true,
		}
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
			f.Docker.Network,
		},
		Cmd: []string{"-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off", "-c", "random_page_cost=1.0"},
	}
	resource, err := f.Docker.Pool.RunWithOptions(&opts)
	f.Resource = resource
	if err != nil {
		return err
	}

	// Tell docker to kill the container after an unreasonable amount of test time to prevent orphans.
	if f.Expire == 0 {
		f.Expire = 600
	}
	f.Resource.Expire(f.Expire)

	f.WaitForReady()

	return nil
}

func (f *Postgres) TearDown() error {
	return f.Docker.Pool.Purge(f.Resource)
}

func (f *Postgres) GetConnection(dbname string) (*sqlx.DB, error) {
	settings := f.Settings.Copy()
	if dbname != "" {
		settings.Database = dbname
	}
	return settings.Init()
}

func (f *Postgres) GetHostIP() string {
	return GetHostIP(f.Resource, f.Docker.Network)
}

func (f *Postgres) GetHostName() string {
	return GetHostName(f.Resource)
}

func (f *Postgres) Psql(cmd []string, mounts []string, quiet bool) (int, error) {
	// We're going to connect over the docker network
	settings := f.Settings.Copy()
	settings.Host = f.GetHostIP()
	psql := &Psql{
		Docker:   f.Docker,
		Settings: settings,
		Mounts:   mounts,
		Cmd:      cmd,
	}
	err := psql.SetUp()
	if err != nil && env.Get().Debug {
		// If there was an issue, and debug is enabled, don't destroy the container.
		return psql.ExitCode, err
	}
	psql.TearDown()
	return psql.ExitCode, err
}

func (f *Postgres) CreateDatabase(name string) (string, error) {
	if name == "" {
		name = namesgenerator.GetRandomName(0)
	}
	fmt.Printf("Create database %v on container %v .. ", name, f.GetHostName())
	exitCode, err := f.Psql([]string{"createdb", "--template=template0", name}, []string{}, false)
	fmt.Printf("%v\n", GetStatusSymbol(exitCode))
	return name, err
}

func (f *Postgres) CopyDatabase(source string, target string) error {
	fmt.Printf("Copy database %v to %v on container %v .. ", source, target, f.GetHostName())
	exitCode, err := f.Psql([]string{"createdb", fmt.Sprintf("--template=%v", source), target}, []string{}, false)
	fmt.Printf("%v\n", GetStatusSymbol(exitCode))
	return err
}

func (f *Postgres) DropDatabase(name string) error {
	fmt.Printf("Drop database %v on container %v .. ", name, f.GetHostName())

	db, err := f.GetConnection(name)
	if err != nil {
		return err
	}

	// Revoke future connections.
	if _, err := db.Exec(fmt.Sprintf("REVOKE CONNECT ON DATABASE %v FROM public", name)); err != nil {
		return err
	}

	// Terminate all connections.
	if _, err := db.Exec("SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"); err != nil {
		return err
	}

	if err := db.Close(); err != nil {
		return err
	}

	exitCode, err := f.Psql([]string{"dropdb", name}, []string{}, false)
	fmt.Printf("%v\n", GetStatusSymbol(exitCode))
	return err
}

func (f *Postgres) LoadSql(path string) error {
	load := func(p string) error {
		dir, err := filepath.Abs(filepath.Dir(p))
		if err != nil {
			return err
		}
		name := filepath.Base(p)
		fmt.Printf("Load %v into database %v on container %v .. ", name, f.Settings.Database, f.GetHostName())
		exitCode, err := f.Psql([]string{"psql", fmt.Sprintf("--file=/tmp/%v", name)}, []string{fmt.Sprintf("%v:/tmp", dir)}, false)
		fmt.Printf("%v\n", GetStatusSymbol(exitCode))
		if err != nil {
			log.Fatalf("Failed to run psql (load sql): %s", err)
			return err
		}
		return nil
	}

	if info, err := os.Stat(path); err == nil {
		if info.IsDir() {
			files, err := filepath.Glob(filepath.Join(path, "*.sql"))
			if err != nil {
				return err
			}
			for _, path := range files {
				if err := load(path); err != nil {
					return err
				}
			}
		} else {
			return load(path)
		}
	}
	return nil
}

// https://github.com/ory/dockertest/blob/v3/examples/PostgreSQL.md
// https://stackoverflow.com/a/63011266
func (f *Postgres) WaitForReady() error {
	if err := f.Docker.Pool.Retry(func() error {
		var err error

		port := f.Resource.GetPort("5432/tcp")
		if port == "" {
			return fmt.Errorf("could not get port from container: %+v", f.Resource.Container)
		}
		f.Settings.Port = port

		status, err := f.Psql([]string{"pg_isready"}, []string{}, true)
		if err != nil {
			return err
		}
		if status != 0 {
			var reason string
			switch status {
			case 1:
				reason = "server is rejecting connections"
			case 2:
				reason = "no response"
			case 3:
				reason = "no attempt was made"
			}
			return fmt.Errorf("postgres is not ready: (%v) %v", status, reason)
		}

		db, err := f.Settings.Open()
		if err != nil {
			return err
		}
		defer db.Close()

		return db.Ping()
	}); err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}

	return nil
}

type Psql struct {
	BaseFixture
	Docker   *Docker
	Settings *ezsqlx.ConnectionSettings
	Resource *dockertest.Resource
	Version  string
	Mounts   []string
	Cmd      []string
	Quiet    bool
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
			f.Docker.Network,
		},
		Cmd: f.Cmd,
	}
	resource, err := f.Docker.Pool.RunWithOptions(&opts)
	f.Resource = resource
	if err != nil {
		return err
	}
	f.ExitCode = WaitForContainer(f.Docker.Pool, f.Resource)
	containerName := f.Resource.Container.Name[1:]
	containerID := f.Resource.Container.ID[0:11]
	if f.ExitCode != 0 && !f.Quiet {
		fmt.Printf("psql (name: %v, id: %v) '%v', exit %v\n", containerName, containerID, f.Cmd, f.ExitCode)
		return fmt.Errorf("psql exited with error (%v): %v", f.ExitCode, getLogs(containerID, f.Docker.Pool))
	}
	return err
}

func (f *Psql) TearDown() error {
	return f.Docker.Pool.Purge(f.Resource)
}

type PostgresDatabaseCopy struct {
	BaseFixture
	Postgres     *Postgres
	Settings     *ezsqlx.ConnectionSettings
	DatabaseName string
}

func (f *PostgresDatabaseCopy) SetUp() error {
	if f.Postgres == nil {
		panic("you must provide an initialized Postgres fixture")
	}

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

func (f *PostgresDatabaseCopy) GetConnection() (*sqlx.DB, error) {
	return f.Postgres.GetConnection(f.DatabaseName)
}

type PostgresSchema struct {
	BaseFixture
	Postgres     *Postgres
	Settings     *ezsqlx.ConnectionSettings
	DatabaseName string
	PathGlob     string
}

func (f *PostgresSchema) SetUp() error {
	// Copy the postgres settings and update them to point at the container's docker network IP and new database
	f.Settings = f.Postgres.Settings.Copy()

	// If no DatabaseName is provided, create a temp database.
	if f.DatabaseName == "" {
		f.DatabaseName = namesgenerator.GetRandomName(0)
		_, err := f.Postgres.CreateDatabase(f.DatabaseName)
		if err != nil {
			log.Fatalf("Failed to create database: %s", err)
			return err
		}
	}
	f.Settings.Database = f.DatabaseName

	// Load schema for this database if it exists.
	// Note: We will load the schema based on the name of the database on the original database connection settings.
	files, err := filepath.Glob(f.PathGlob)
	if err != nil {
		return err
	}
	for _, path := range files {
		err := f.Postgres.LoadSql(path)
		if err != nil {
			log.Fatalf("Failed to load test data: %s", err)
			return err
		}
	}

	return nil
}

func (f *PostgresSchema) TearDown() error {
	return f.Postgres.DropDatabase(f.DatabaseName)
}

func (f *PostgresSchema) GetConnection() (*sqlx.DB, error) {
	return f.Postgres.GetConnection(f.DatabaseName)
}

type PostgresWithSchema struct {
	BaseFixture
	Docker   *Docker
	Settings *ezsqlx.ConnectionSettings
	Version  string
	PathGlob string
	Postgres *Postgres
	Schema   *PostgresSchema
}

func (f *PostgresWithSchema) SetUp() error {
	var err error

	if f.Postgres == nil {
		f.Postgres = &Postgres{
			Docker:   f.Docker,
			Settings: f.Settings,
			Version:  f.Version,
		}
		err = f.Postgres.SetUp()
		if err != nil {
			return err
		}
	}

	if f.Settings == nil {
		f.Settings = f.Postgres.Settings
	}

	f.Schema = &PostgresSchema{
		Postgres:     f.Postgres,
		DatabaseName: f.Settings.Database,
		PathGlob:     f.PathGlob,
	}
	err = f.Schema.SetUp()

	return err
}

func (f *PostgresWithSchema) GetConnection() (*sqlx.DB, error) {
	return f.Schema.GetConnection()
}

func (f *PostgresWithSchema) TearDown() error {
	// Don't bother tearing down schema since it does psql teardown after each run
	// and we're about to destroy the whole database anyways.
	// f.PostgresSchema.TearDown()
	return f.Postgres.TearDown()
}
