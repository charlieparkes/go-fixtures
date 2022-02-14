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
		docker: d,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func PostgresDocker(d *Docker) PostgresOpt {
	return func(f *Postgres) {
		f.docker = d
	}
}

func PostgresSettings(settings *ConnectionSettings) PostgresOpt {
	return func(f *Postgres) {
		f.settings = settings
	}
}

func PostgresRepo(repo string) PostgresOpt {
	return func(f *Postgres) {
		f.repo = repo
	}
}

func PostgresVersion(version string) PostgresOpt {
	return func(f *Postgres) {
		f.version = version
	}
}

// Tell docker to kill the container after an unreasonable amount of test time to prevent orphans. Defaults to 600 seconds.
func PostgresExpireAfter(expireAfter uint) PostgresOpt {
	return func(f *Postgres) {
		f.expireAfter = expireAfter
	}
}

// Wait this long for operations to execute. Defaults to 30 seconds.
func PostgresTimeoutAfter(timeoutAfter uint) PostgresOpt {
	return func(f *Postgres) {
		f.timeoutAfter = timeoutAfter
	}
}

func PostgresSkipTearDown() PostgresOpt {
	return func(f *Postgres) {
		f.skipTearDown = true
	}
}

func PostgresMounts(mounts []string) PostgresOpt {
	return func(f *Postgres) {
		f.mounts = mounts
	}
}

type Postgres struct {
	BaseFixture
	log          *zap.Logger
	docker       *Docker
	settings     *ConnectionSettings
	resource     *dockertest.Resource
	repo         string
	version      string
	expireAfter  uint
	timeoutAfter uint
	skipTearDown bool
	mounts       []string
}

func (f *Postgres) GetSettings() *ConnectionSettings {
	return f.settings
}

func (f *Postgres) SetUp(ctx context.Context) error {
	f.log = logger()
	if f.repo == "" {
		f.repo = DEFAULT_POSTGRES_REPO
	}
	if f.version == "" {
		f.version = DEFAULT_POSTGRES_VERSION
	}
	if f.settings == nil {
		f.settings = &ConnectionSettings{
			User:       "postgres",
			Password:   GenerateString(),
			Database:   f.docker.GetNamePrefix(),
			DisableSSL: true,
		}
	}
	networks := make([]*dockertest.Network, 0)
	if f.docker.GetNetwork() != nil {
		networks = append(networks, f.docker.GetNetwork())
	}
	opts := dockertest.RunOptions{
		Repository: f.repo,
		Tag:        f.version,
		Env: []string{
			"POSTGRES_USER=" + f.settings.User,
			"POSTGRES_PASSWORD=" + f.settings.Password,
			"POSTGRES_DB=" + f.settings.Database,
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
		Mounts: f.mounts,
	}
	var err error
	f.resource, err = f.docker.GetPool().RunWithOptions(&opts)
	if err != nil {
		return err
	}

	f.settings.Host = GetContainerAddress(f.resource, f.docker.GetNetwork())

	if f.expireAfter == 0 {
		f.expireAfter = 600
	}
	f.resource.Expire(f.expireAfter)

	if f.timeoutAfter == 0 {
		f.timeoutAfter = 15
	}
	if err := f.WaitForReady(ctx, time.Second*time.Duration(f.timeoutAfter)); err != nil {
		return err
	}
	return nil
}

func (f *Postgres) TearDown(ctx context.Context) error {
	defer f.log.Sync()
	if f.skipTearDown {
		return nil
	}
	wg.Add(1)
	go purge(f.docker.GetPool(), f.resource)
	return nil
}

func (f *Postgres) GetConnection(ctx context.Context, database string) (*pgx.Conn, error) {
	settings := f.settings.Copy()
	if database != "" {
		settings.Database = database
	}
	return settings.Connect(ctx)
}

func (f *Postgres) GetHostName() string {
	return GetHostName(f.resource)
}

func (f *Postgres) Psql(ctx context.Context, cmd []string, mounts []string, quiet bool) (int, error) {
	// We're going to connect over the docker network
	settings := f.settings.Copy()
	settings.Host = GetHostIP(f.resource, f.docker.GetNetwork())
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
			f.docker.GetNetwork(),
		},
		Cmd: cmd,
	}
	// f.log.Debug("psql setup", zap.Any("environment", opts.Env))
	resource, err := f.docker.GetPool().RunWithOptions(&opts)
	if err != nil {
		return 0, err
	}
	exitCode, err := WaitForContainer(f.docker.GetPool(), resource)
	containerName := resource.Container.Name[1:]
	containerID := resource.Container.ID[0:11]
	if err != nil || exitCode != 0 && !quiet {
		f.log.Debug("psql failed", zap.Int("status", exitCode), zap.String("container_name", containerName), zap.String("container_id", containerID), zap.String("cmd", strings.Join(cmd, " ")))
		return exitCode, fmt.Errorf("psql exited with error (%v): %v", exitCode, getLogs(f.log, containerID, f.docker.GetPool()))
	}
	if f.skipTearDown && getEnv().Debug {
		// If there was an issue, and debug is enabled, don't destroy the container.
		return exitCode, nil
	}
	wg.Add(1)
	go purge(f.docker.GetPool(), resource)
	return exitCode, nil
}

func (f *Postgres) PingPsql(ctx context.Context) error {
	_, err := f.Psql(ctx, []string{"psql", "-c", ";"}, []string{}, false)
	return err
}

func (f *Postgres) Ping(ctx context.Context) error {
	db, err := f.GetConnection(ctx, "")
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	return db.Ping(ctx)
}

func (f *Postgres) CreateDatabase(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("must provide a database name")
	}
	exitCode, err := f.Psql(ctx, []string{"createdb", "--template=template0", name}, []string{}, false)
	f.log.Debug("create database", zap.Int("status", exitCode), zap.String("database", name), zap.String("container", f.GetHostName()))
	return err
}

// CopyDatabase creates a copy of an existing postgres database using `createdb --template={source} {target}`
// source will default to the primary database
func (f *Postgres) CopyDatabase(ctx context.Context, source string, target string) error {
	if source == "" {
		source = f.settings.Database
	}
	exitCode, err := f.Psql(ctx, []string{"createdb", fmt.Sprintf("--template=%v", source), target}, []string{}, false)
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

	exitCode, err := f.Psql(ctx, []string{"dropdb", name}, []string{}, false)
	f.log.Debug("drop database", zap.Int("status", exitCode), zap.String("database", name), zap.String("container", f.GetHostName()))
	return err
}

func (f *Postgres) Dump(ctx context.Context, dir string, filename string) error {
	path := FindPath(dir)
	if path == "" {
		return fmt.Errorf("could not resolve path: %v", dir)
	}
	exitCode, err := f.Psql(ctx, []string{"sh", "-c", fmt.Sprintf("pg_dump -Fc -Z0 %v > /tmp/%v", f.settings.Database, filename)}, []string{fmt.Sprintf("%v:/tmp", path)}, false)
	f.log.Debug("dump database", zap.Int("status", exitCode), zap.String("database", f.settings.Database), zap.String("container", f.GetHostName()), zap.String("path", path))
	return err
}

func (f *Postgres) Restore(ctx context.Context, dir string, filename string) error {
	path := FindPath(dir)
	if path == "" {
		return fmt.Errorf("could not resolve path: %v", dir)
	}
	exitCode, err := f.Psql(ctx, []string{"sh", "-c", fmt.Sprintf("pg_restore --dbname=%v --verbose --single-transaction /tmp/%v", f.settings.Database, filename)}, []string{fmt.Sprintf("%v:/tmp", path)}, false)
	f.log.Debug("restore database", zap.Int("status", exitCode), zap.String("database", f.settings.Database), zap.String("container", f.GetHostName()), zap.String("path", path))
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
		exitCode, err := f.Psql(ctx, []string{"psql", fmt.Sprintf("--file=/tmp/%v", name)}, []string{fmt.Sprintf("%v:/tmp", dir)}, false)
		f.log.Debug("load sql", zap.Int("status", exitCode), zap.String("database", f.settings.Database), zap.String("container", f.GetHostName()), zap.String("name", name))
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

		port := GetContainerTcpPort(f.resource, f.docker.GetNetwork(), "5432")
		if port == "" {
			err = fmt.Errorf("could not get port from container: %+v", f.resource.Container)
			return err
		}
		f.settings.Port = port

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

		db, err := f.settings.Connect(ctx)
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
	defer db.Close(ctx)
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
	defer db.Close(ctx)
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
	defer db.Close(ctx)
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
