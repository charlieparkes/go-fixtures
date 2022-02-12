package fixtures

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"
)

const DEFAULT_POSTGRES_REPO = "postgres"
const DEFAULT_POSTGRES_VERSION = "13-alpine"

type PostgresOpt func(*Postgres)

func NewPostgres(d *Docker, opts ...PostgresOpt) *Postgres {
	f := &Postgres{
		Docker: d,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func PostgresOptDocker(d *Docker) PostgresOpt {
	return func(f *Postgres) {
		f.Docker = d
	}
}

func PostgresOptSettings(settings *ConnectionSettings) PostgresOpt {
	return func(f *Postgres) {
		f.Settings = settings
	}
}

func PostgresOptRepo(repo string) PostgresOpt {
	return func(f *Postgres) {
		f.Repo = repo
	}
}

func PostgresOptVersion(version string) PostgresOpt {
	return func(f *Postgres) {
		f.Version = version
	}
}

func PostgresOptExpireAfter(expireAfter uint) PostgresOpt {
	return func(f *Postgres) {
		f.ExpireAfter = expireAfter
	}
}

func PostgresOptTimeoutAfter(timeoutAfter uint) PostgresOpt {
	return func(f *Postgres) {
		f.TimeoutAfter = timeoutAfter
	}
}

func PostgresOptSkipTearDown() PostgresOpt {
	return func(f *Postgres) {
		f.SkipTearDown = true
	}
}

type Postgres struct {
	BaseFixture
	log          *zap.Logger
	Docker       *Docker
	Settings     *ConnectionSettings
	Resource     *dockertest.Resource
	Repo         string
	Version      string
	ExpireAfter  uint // Tell docker to kill the container after an unreasonable amount of test time to prevent orphans. Defaults to 600 seconds.
	TimeoutAfter uint // Wait this long for operations to execute. Defaults to 30 seconds.
	SkipTearDown bool
	Mounts       []string
}

func (f *Postgres) SetUp(ctx context.Context) error {
	f.log = logger()
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
			Database:   f.Docker.GetNamePrefix(),
			DisableSSL: true,
		}
	}
	networks := make([]*dockertest.Network, 0)
	if f.Docker.GetNetwork() != nil {
		networks = append(networks, f.Docker.GetNetwork())
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
	f.Resource, err = f.Docker.GetPool().RunWithOptions(&opts)
	if err != nil {
		return err
	}

	f.Settings.Host = GetContainerAddress(f.Resource, f.Docker.GetNetwork())

	if f.ExpireAfter == 0 {
		f.ExpireAfter = 600
	}
	f.Resource.Expire(f.ExpireAfter)

	if f.TimeoutAfter == 0 {
		f.TimeoutAfter = 15
	}
	if err := f.WaitForReady(ctx, time.Second*time.Duration(f.TimeoutAfter)); err != nil {
		return err
	}
	return nil
}

func (f *Postgres) TearDown(ctx context.Context) error {
	defer f.log.Sync()
	if f.SkipTearDown {
		return nil
	}
	wg.Add(1)
	go purge(f.Docker.GetPool(), f.Resource)
	return nil
}

func (f *Postgres) GetConnection(ctx context.Context, database string) (*pgx.Conn, error) {
	settings := f.Settings.Copy()
	if database != "" {
		settings.Database = database
	}
	return settings.Connect(ctx)
}

func (f *Postgres) GetHostName() string {
	return GetHostName(f.Resource)
}

func (f *Postgres) Psql(ctx context.Context, cmd []string, mounts []string, quiet bool) (int, error) {
	// We're going to connect over the docker network
	settings := f.Settings.Copy()
	settings.Host = GetHostIP(f.Resource, f.Docker.GetNetwork())
	var err error
	opts := dockertest.RunOptions{
		Repository: "governmentpaas/psql",
		Tag:        "latest",
		Env: []string{
			"PGUSER=" + settings.User,
			"PGPASSWORD=" + settings.Password,
			"PGDATABASE=" + settings.Database,
			"PGHOST=" + settings.Host,
			"PGPORT=5432",
		},
		Mounts: mounts,
		Networks: []*dockertest.Network{
			f.Docker.GetNetwork(),
		},
		Cmd: cmd,
	}
	resource, err := f.Docker.GetPool().RunWithOptions(&opts)
	if err != nil {
		return 0, err
	}
	exitCode, err := WaitForContainer(f.Docker.GetPool(), resource)
	containerName := resource.Container.Name[1:]
	containerID := resource.Container.ID[0:11]
	if err != nil || exitCode != 0 && !quiet {
		f.log.Debug("psql failed", zap.Int("status", exitCode), zap.String("container_name", containerName), zap.String("container_id", containerID), zap.String("cmd", strings.Join(cmd, " ")))
		return exitCode, fmt.Errorf("psql exited with error (%v): %v", exitCode, getLogs(f.log, containerID, f.Docker.GetPool()))
	}
	if f.SkipTearDown && getEnv().Debug {
		// If there was an issue, and debug is enabled, don't destroy the container.
		return exitCode, nil
	}
	wg.Add(1)
	go purge(f.Docker.GetPool(), resource)
	return exitCode, nil
}

func (f *Postgres) CreateDatabase(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("must provide a database name")
	}
	var exitCode int
	err := Retry(time.Second*time.Duration(f.TimeoutAfter), func() error {
		var err error
		exitCode, err = f.Psql(ctx, []string{"createdb", "--template=template0", name}, []string{}, false)
		return err
	})
	f.log.Debug("create database", zap.Int("status", exitCode), zap.String("database", name), zap.String("container", f.GetHostName()))
	return err
}

// CopyDatabase creates a copy of an existing postgres database using createdb --template
// ex.
//		name := namesgenerator.GetRandomName(0)
//		f.CopyDatabase(ctx, "my_db", name)
// 		f.GetConnection(ctx, name)
func (f *Postgres) CopyDatabase(ctx context.Context, source string, target string) error {
	if source == "" {
		source = f.Settings.Database
	}
	var exitCode int
	err := Retry(time.Second*time.Duration(f.TimeoutAfter), func() error {
		var err error
		exitCode, err = f.Psql(ctx, []string{"createdb", fmt.Sprintf("--template=%v", source), target}, []string{}, false)
		return err
	})
	f.log.Debug("copy database", zap.Int("status", exitCode), zap.String("source", source), zap.String("target", target), zap.String("container", f.GetHostName()))
	return err
}

func (f *Postgres) DropDatabase(ctx context.Context, name string) error {
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

	var exitCode int
	err = Retry(time.Second*time.Duration(f.TimeoutAfter), func() error {
		var err error
		exitCode, err = f.Psql(ctx, []string{"dropdb", name}, []string{}, false)
		return err
	})
	f.log.Debug("drop database", zap.Int("status", exitCode), zap.String("database", name), zap.String("container", f.GetHostName()))
	return err
}

func (f *Postgres) Dump(ctx context.Context, dir string, filename string) error {
	path := FindPath(dir)
	if path == "" {
		return fmt.Errorf("could not resolve path: %v", dir)
	}
	var exitCode int
	err := Retry(time.Second*time.Duration(f.TimeoutAfter), func() error {
		var err error
		exitCode, err = f.Psql(ctx, []string{"sh", "-c", fmt.Sprintf("pg_dump -Fc -Z0 %v > /tmp/%v", f.Settings.Database, filename)}, []string{fmt.Sprintf("%v:/tmp", path)}, false)
		return err
	})
	f.log.Debug("dump database", zap.Int("status", exitCode), zap.String("database", f.Settings.Database), zap.String("container", f.GetHostName()), zap.String("path", path))
	return err
}

func (f *Postgres) Restore(ctx context.Context, dir string, filename string) error {
	path := FindPath(dir)
	if path == "" {
		return fmt.Errorf("could not resolve path: %v", dir)
	}
	var exitCode int
	err := Retry(time.Second*time.Duration(f.TimeoutAfter), func() error {
		var err error
		exitCode, err = f.Psql(ctx, []string{"sh", "-c", fmt.Sprintf("pg_restore --dbname=%v --verbose --single-transaction /tmp/%v", f.Settings.Database, filename)}, []string{fmt.Sprintf("%v:/tmp", path)}, false)
		return err
	})
	f.log.Debug("restore database", zap.Int("status", exitCode), zap.String("database", f.Settings.Database), zap.String("container", f.GetHostName()), zap.String("path", path))
	return err
}

// LoadSql runs a file or directory of *.sql files against the default postgres database.
func (f *Postgres) LoadSql(ctx context.Context, path string) error {
	load := func(p string) error {
		dir, err := filepath.Abs(filepath.Dir(p))
		if err != nil {
			return err
		}
		name := filepath.Base(p)
		var exitCode int
		err = Retry(time.Second*time.Duration(f.TimeoutAfter), func() error {
			var err error
			exitCode, err = f.Psql(ctx, []string{"psql", fmt.Sprintf("--file=/tmp/%v", name)}, []string{fmt.Sprintf("%v:/tmp", dir)}, false)
			return err
		})
		f.log.Debug("load sql", zap.Int("status", exitCode), zap.String("database", f.Settings.Database), zap.String("container", f.GetHostName()), zap.String("name", name))
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

// LoadSqlPattern finds files matching a custom pattern and runs them against the default database.
func (f *Postgres) LoadSqlPattern(ctx context.Context, pattern string) error {
	// Load schema for this database if it exists.
	// Note: We will load the schema based on the name of the database on the original database connection settings.
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, path := range files {
		err := f.LoadSql(ctx, path)
		if err != nil {
			return fmt.Errorf("failed to load test data: %w", err)
		}
	}
	return nil
}

// https://github.com/ory/dockertest/blob/v3/examples/PostgreSQL.md
// https://stackoverflow.com/a/63011266
func (f *Postgres) WaitForReady(ctx context.Context, d time.Duration) error {
	if err := Retry(d, func() error {
		var err error

		port := GetContainerTcpPort(f.Resource, f.Docker.GetNetwork(), "5432")
		if port == "" {
			err = fmt.Errorf("could not get port from container: %+v", f.Resource.Container)
			return err
		}
		f.Settings.Port = port

		status, err := f.Psql(ctx, []string{"pg_isready"}, []string{}, true)
		if err != nil {
			return err
		}
		if status != 0 {
			reason := "unknown"
			switch status {
			case 1:
				reason = "server is rejecting connections"
			case 2:
				reason = "no response"
			case 3:
				reason = "no attempt was made"
			}
			err = fmt.Errorf("postgres is not ready: (%v) %v", status, reason)
			return err
		}

		db, err := f.Settings.Connect(ctx)
		if err != nil {
			return err
		}
		return db.Close(ctx)
	}); err != nil {
		return fmt.Errorf("gave up waiting for postgres: %w", err)
	}

	return nil
}

func (f *Postgres) TableExists(ctx context.Context, database, schema, table string) (bool, error) {
	db, err := f.GetConnection(ctx, database)
	if err != nil {
		return false, err
	}
	query := "SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename = $2"
	count := 0
	if err := db.QueryRow(ctx, query, schema, table).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func (f *Postgres) GetTableColumns(ctx context.Context, database, schema, table string) ([]string, error) {
	db, err := f.GetConnection(ctx, database)
	if err != nil {
		return nil, err
	}
	var columnNames pgtype.TextArray
	query := fmt.Sprintf("SELECT array_agg(column_name::text) FROM information_schema.columns WHERE table_schema = '%v' AND table_name = '%v'", schema, table)
	if err := db.QueryRow(ctx,
		query,
	).Scan(&columnNames); err != nil {
		return nil, err
	}
	cols := make([]string, len(columnNames.Elements))
	for _, text := range columnNames.Elements {
		cols = append(cols, text.String)
	}
	return cols, nil
}

func (f *Postgres) GetTables(ctx context.Context, database string) ([]string, error) {
	db, err := f.GetConnection(ctx, database)
	if err != nil {
		return nil, err
	}
	tables := []string{}
	rows, err := db.Query(ctx, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'information_schema' AND schemaname != 'pg_catalog'")
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return nil, fmt.Errorf("failed to scan: %w", err)
		}
		tables = append(tables, table)
	}
	return tables, nil
}
