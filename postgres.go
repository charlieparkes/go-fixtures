package fixtures

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/jackc/pgx/v4"
	"github.com/ory/dockertest/v3"
)

const DEFAULT_POSTGRES_REPO = "postgres"
const DEFAULT_POSTGRES_VERSION = "13-alpine"

type Postgres struct {
	BaseFixture
	Docker       *Docker
	Settings     *ConnectionSettings
	Resource     *dockertest.Resource
	Repo         string
	Version      string
	ExpireAfter  uint // seconds, default 600
	TimeoutAfter uint // seconds, default 30
	SkipTearDown bool
	Mounts       []string
}

func (f *Postgres) SetUp(ctx context.Context) error {
	if f.Repo == "" {
		f.Repo = DEFAULT_POSTGRES_REPO
	}
	if f.Version == "" {
		f.Version = DEFAULT_POSTGRES_VERSION
	}
	if f.Settings == nil {
		f.Settings = &ConnectionSettings{
			User:       "postgres",
			Password:   GenerateString(),
			Database:   f.Docker.NamePrefix,
			DisableSSL: true,
		}
	}
	networks := make([]*dockertest.Network, 0)
	if f.Docker.Network != nil {
		networks = append(networks, f.Docker.Network)
	}
	opts := dockertest.RunOptions{
		Repository: f.Repo,
		Tag:        f.Version,
		Env: []string{
			"POSTGRES_USER=" + f.Settings.User,
			"POSTGRES_PASSWORD=" + f.Settings.Password,
			"POSTGRES_DB=" + f.Settings.Database,
		},
		Networks: networks,
		Cmd: []string{
			// https://www.postgresql.org/docs/current/non-durability.html
			"-c", "fsync=off",
			"-c", "synchronous_commit=off",
			"-c", "full_page_writes=off",
			"-c", "random_page_cost=1.1",
			"-c", fmt.Sprintf("shared_buffers=%vMB", memoryMB()/8),
			"-c", fmt.Sprintf("work_mem=%vMB", memoryMB()/8),
		},
		Mounts: f.Mounts,
	}
	var err error
	f.Resource, err = f.Docker.Pool.RunWithOptions(&opts)
	if err != nil {
		return err
	}

	f.Settings.Host = GetContainerAddress(f.Resource, f.Docker.Network)

	// Tell docker to kill the container after an unreasonable amount of test time to prevent orphans.
	if f.ExpireAfter == 0 {
		f.ExpireAfter = 600
	}
	f.Resource.Expire(f.ExpireAfter)

	if f.TimeoutAfter == 0 {
		f.TimeoutAfter = 30
	}
	f.WaitForReady(ctx, time.Second*time.Duration(f.TimeoutAfter))

	return nil
}

func (f *Postgres) TearDown(ctx context.Context) error {
	if f.SkipTearDown {
		return nil
	}
	wg.Add(1)
	go purge(f.Docker.Pool, f.Resource)
	return nil
}

func (f *Postgres) GetConnection(ctx context.Context, dbname string) (*pgx.Conn, error) {
	settings := f.Settings.Copy()
	if dbname != "" {
		settings.Database = dbname
	}
	return settings.Connect(ctx)
}

func (f *Postgres) GetHostIP() string {
	return GetHostIP(f.Resource, f.Docker.Network)
}

func (f *Postgres) GetHostName() string {
	return GetHostName(f.Resource)
}

func (f *Postgres) Psql(ctx context.Context, cmd []string, mounts []string, quiet bool, skipTearDown bool) (int, error) {
	// We're going to connect over the docker network
	settings := f.Settings.Copy()
	settings.Host = f.GetHostIP()
	psql := &Psql{
		Docker:       f.Docker,
		Settings:     settings,
		Mounts:       mounts,
		Cmd:          cmd,
		SkipTearDown: skipTearDown,
	}
	err := psql.SetUp(ctx)
	if err != nil && getEnv().Debug {
		// If there was an issue, and debug is enabled, don't destroy the container.
		return psql.ExitCode, err
	}
	psql.TearDown(ctx)
	return psql.ExitCode, err
}

func (f *Postgres) CreateDatabase(ctx context.Context, name string) (string, error) {
	if name == "" {
		name = namesgenerator.GetRandomName(0)
	}
	debugPrintf("Create database %v on container %v .. ", name, f.GetHostName())
	exitCode, err := f.Psql(ctx, []string{"createdb", "--template=template0", name}, []string{}, false, false)
	debugPrintf("%v\n", GetStatusSymbol(exitCode))
	return name, err
}

func (f *Postgres) CopyDatabase(ctx context.Context, source string, target string) error {
	debugPrintf("Copy database %v to %v on container %v .. ", source, target, f.GetHostName())
	exitCode, err := f.Psql(ctx, []string{"createdb", fmt.Sprintf("--template=%v", source), target}, []string{}, false, false)
	debugPrintf("%v\n", GetStatusSymbol(exitCode))
	return err
}

func (f *Postgres) DropDatabase(ctx context.Context, name string) error {
	debugPrintf("Drop database %v on container %v .. ", name, f.GetHostName())

	db, err := f.GetConnection(ctx, name)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	// Revoke future connections.
	_, err = db.Exec(ctx, fmt.Sprintf("REVOKE CONNECT ON DATABASE %v FROM public", name))
	if err != nil {
		return err
	}

	// Terminate all connections.
	_, err = db.Exec(ctx, "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()")
	if err != nil {
		return err
	}

	exitCode, err := f.Psql(ctx, []string{"dropdb", name}, []string{}, false, false)
	debugPrintf("%v\n", GetStatusSymbol(exitCode))
	return err
}

func (f *Postgres) DumpDatabase(ctx context.Context, dir string, filename string) error {
	path := FindPath(dir)
	if path == "" {
		return fmt.Errorf("could not resolve path: %v", dir)
	}
	debugPrintf("Dump database %v on container %v to %v.. ", f.Settings.Database, f.GetHostName(), path)
	exitCode, err := f.Psql(ctx, []string{"sh", "-c", fmt.Sprintf("pg_dump -Fc -Z0 %v > /tmp/%v", f.Settings.Database, filename)}, []string{fmt.Sprintf("%v:/tmp", path)}, false, false)
	debugPrintf("%v\n", GetStatusSymbol(exitCode))
	return err
}

func (f *Postgres) RestoreDatabase(ctx context.Context, dir string, filename string) error {
	path := FindPath(dir)
	if path == "" {
		return fmt.Errorf("could not resolve path: %v", dir)
	}
	debugPrintf("Restore database %v on container %v to %v.. ", f.Settings.Database, f.GetHostName(), path)
	exitCode, err := f.Psql(ctx, []string{"sh", "-c", fmt.Sprintf("pg_restore --dbname=%v --verbose --single-transaction /tmp/%v", f.Settings.Database, filename)}, []string{fmt.Sprintf("%v:/tmp", path)}, false, false)
	debugPrintf("%v\n", GetStatusSymbol(exitCode))
	return err
}

func (f *Postgres) LoadSql(ctx context.Context, path string) error {
	load := func(p string) error {
		dir, err := filepath.Abs(filepath.Dir(p))
		if err != nil {
			return err
		}
		name := filepath.Base(p)
		debugPrintf("Load %v into database %v on container %v .. ", name, f.Settings.Database, f.GetHostName())
		exitCode, err := f.Psql(ctx, []string{"psql", fmt.Sprintf("--file=/tmp/%v", name)}, []string{fmt.Sprintf("%v:/tmp", dir)}, false, false)
		debugPrintf("%v\n", GetStatusSymbol(exitCode))
		if err != nil {
			return fmt.Errorf("failed to run psql (load sql): %w", err)
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
func (f *Postgres) WaitForReady(ctx context.Context, d time.Duration) error {
	if err := Retry(d, func() error {
		var err error

		port := GetContainerTcpPort(f.Resource, f.Docker.Network, "5432")
		if port == "" {
			err = fmt.Errorf("could not get port from container: %+v", f.Resource.Container)
			debugPrintln(err)
			return err
		}
		f.Settings.Port = port

		status, err := f.Psql(ctx, []string{"pg_isready"}, []string{}, true, false)
		if err != nil {
			debugPrintln(err)
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
			err = fmt.Errorf("postgres is not ready: (%v) %v", status, reason)
			debugPrintln(err)
			return err
		}

		db, err := f.Settings.Connect(ctx)
		if err != nil {
			debugPrintln(err)
			return err
		}
		return db.Close(ctx)
	}); err != nil {
		return fmt.Errorf("gave up waiting for postgres: %w", err)
	}

	return nil
}

type Psql struct {
	BaseFixture
	Docker       *Docker
	Settings     *ConnectionSettings
	Resource     *dockertest.Resource
	Version      string
	Mounts       []string
	Cmd          []string
	Quiet        bool
	ExitCode     int
	SkipTearDown bool
}

func (f *Psql) SetUp(ctx context.Context) error {
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
	f.ExitCode, err = WaitForContainer(f.Docker.Pool, f.Resource)
	containerName := f.Resource.Container.Name[1:]
	containerID := f.Resource.Container.ID[0:11]
	if err != nil || f.ExitCode != 0 && !f.Quiet {
		debugPrintf("psql (name: %v, id: %v) '%v', exit %v\n", containerName, containerID, f.Cmd, f.ExitCode)
		return fmt.Errorf("psql exited with error (%v): %v", f.ExitCode, getLogs(containerID, f.Docker.Pool))
	}
	return err
}

func (f *Psql) TearDown(ctx context.Context) error {
	if f.SkipTearDown {
		return nil
	}
	wg.Add(1)
	go purge(f.Docker.Pool, f.Resource)
	return nil
}

type PostgresDatabaseCopy struct {
	BaseFixture
	Postgres     *Postgres
	Settings     *ConnectionSettings
	DatabaseName string
	SkipTearDown bool
}

func (f *PostgresDatabaseCopy) SetUp(ctx context.Context) error {
	if f.Postgres == nil {
		panic("you must provide an initialized Postgres fixture")
	}

	// Copy the postgres settings and update them to point at the container's docker network IP and new database
	f.Settings = f.Postgres.Settings.Copy()

	// If no DatabaseName is provided, create a database copy.
	if f.DatabaseName == "" {
		f.DatabaseName = namesgenerator.GetRandomName(0)
		err := f.Postgres.CopyDatabase(ctx, f.Settings.Database, f.DatabaseName)
		if err != nil {
			return fmt.Errorf("failed to copy database: %w", err)
		}
	}
	f.Settings.Database = f.DatabaseName
	return nil
}

func (f *PostgresDatabaseCopy) TearDown(ctx context.Context) error {
	if f.SkipTearDown {
		return nil
	}
	return f.Postgres.DropDatabase(ctx, f.DatabaseName)
}

func (f *PostgresDatabaseCopy) GetConnection(ctx context.Context) (*pgx.Conn, error) {
	return f.Postgres.GetConnection(ctx, f.DatabaseName)
}

type PostgresSchema struct {
	BaseFixture
	Postgres     *Postgres
	Settings     *ConnectionSettings
	DatabaseName string
	PathGlob     string
	SkipTearDown bool
}

func (f *PostgresSchema) SetUp(ctx context.Context) error {
	// Copy the postgres settings and update them to point at the container's docker network IP and new database
	f.Settings = f.Postgres.Settings.Copy()

	// If no DatabaseName is provided, create a temp database.
	if f.DatabaseName == "" {
		f.DatabaseName = namesgenerator.GetRandomName(0)
		_, err := f.Postgres.CreateDatabase(ctx, f.DatabaseName)
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
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
		err := f.Postgres.LoadSql(ctx, path)
		if err != nil {
			return fmt.Errorf("failed to load test data: %w", err)
		}
	}

	return nil
}

func (f *PostgresSchema) TearDown(ctx context.Context) error {
	if f.SkipTearDown {
		return nil
	}
	return f.Postgres.DropDatabase(ctx, f.DatabaseName)
}

func (f *PostgresSchema) GetConnection(ctx context.Context) (*pgx.Conn, error) {
	return f.Postgres.GetConnection(ctx, f.DatabaseName)
}

type PostgresWithSchema struct {
	BaseFixture
	Docker   *Docker
	Settings *ConnectionSettings
	Version  string
	PathGlob string
	Postgres *Postgres
	Schema   *PostgresSchema
}

func (f *PostgresWithSchema) SetUp(ctx context.Context) error {
	var err error

	if f.Postgres == nil {
		f.Postgres = &Postgres{
			Docker:   f.Docker,
			Settings: f.Settings,
			Version:  f.Version,
		}
		err = f.Postgres.SetUp(ctx)
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
		SkipTearDown: true,
	}
	err = f.Schema.SetUp(ctx)

	return err
}

func (f *PostgresWithSchema) GetConnection(ctx context.Context) (*pgx.Conn, error) {
	return f.Schema.GetConnection(ctx)
}

func (f *PostgresWithSchema) TearDown(ctx context.Context) error {
	// Don't bother tearing down schema since it does psql teardown after each run
	// and we're about to destroy the whole database anyways.
	// f.PostgresSchema.TearDown(ctx)
	return f.Postgres.TearDown(ctx)
}
