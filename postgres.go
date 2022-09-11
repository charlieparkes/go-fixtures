package fixtures

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charlieparkes/go-structs"
	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/iancoleman/strcase"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"
)

const (
	DEFAULT_POSTGRES_REPO    = "postgres"
	DEFAULT_POSTGRES_VERSION = "13-alpine"
)

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

func PostgresLogger(logger *zap.Logger) PostgresOpt {
	return func(f *Postgres) {
		f.log = logger
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

func (f *Postgres) Settings() *ConnectionSettings {
	return f.settings
}

func (f *Postgres) ConnConfig() (*pgxpool.Config, error) {
	return pgxpool.ParseConfig(f.settings.String())
}

func (f *Postgres) SetUp(ctx context.Context) error {
	if f.log == nil {
		f.log = logger()
	}
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
			Database:   f.docker.NamePrefix(),
			DisableSSL: true,
		}
	}
	networks := make([]*dockertest.Network, 0)
	if f.docker.Network() != nil {
		networks = append(networks, f.docker.Network())
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
			"-c", fmt.Sprintf("shared_buffers=%vMB", MemoryMB()/8),
			"-c", fmt.Sprintf("work_mem=%vMB", MemoryMB()/8),
		},
		Mounts: f.mounts,
	}
	var err error
	f.resource, err = f.docker.Pool().RunWithOptions(&opts)
	if err != nil {
		return err
	}

	f.settings.Host = ContainerAddress(f.resource, f.docker.Network())

	if f.expireAfter == 0 {
		f.expireAfter = 600
	}
	f.resource.Expire(f.expireAfter)

	if f.timeoutAfter == 0 {
		f.timeoutAfter = 30
	}
	if err := f.WaitForReady(ctx, time.Second*time.Duration(f.timeoutAfter)); err != nil {
		return err
	}
	return nil
}

func (f *Postgres) TearDown(ctx context.Context) error {
	if f.skipTearDown {
		return nil
	}
	f.docker.Purge(f.resource)
	return nil
}

type PostgresConnConfig struct {
	poolConfig *pgxpool.Config
	role       string
	database   string
	createCopy bool
}

type PostgresConnOpt func(*PostgresConnConfig)

func PostgresConnRole(role string) PostgresConnOpt {
	return func(f *PostgresConnConfig) {
		f.role = role
	}
}

func PostgresConnDatabase(database string) PostgresConnOpt {
	return func(f *PostgresConnConfig) {
		if database != "" {
			f.database = database
		}
	}
}

func PostgresConnCreateCopy() PostgresConnOpt {
	return func(f *PostgresConnConfig) {
		f.createCopy = true
	}
}

func (f *Postgres) Connect(ctx context.Context, opts ...PostgresConnOpt) (*pgxpool.Pool, error) {
	poolConfig, err := f.ConnConfig()
	if err != nil {
		return nil, err
	}
	cfg := &PostgresConnConfig{
		poolConfig: poolConfig,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.database != "" {
		cfg.poolConfig.ConnConfig.Database = cfg.database
	}
	if cfg.createCopy {
		copiedDatabaseName := namesgenerator.GetRandomName(0)
		if err := f.CopyDatabase(ctx, cfg.database, copiedDatabaseName); err != nil {
			return nil, err
		}
		cfg.poolConfig.ConnConfig.Database = copiedDatabaseName
	}
	pool, err := pgxpool.ConnectConfig(ctx, cfg.poolConfig)
	if err != nil {
		return nil, err
	}
	if cfg.role != "" {
		_, err := pool.Exec(ctx, "set role "+cfg.role)
		if err != nil {
			return nil, fmt.Errorf("failed to assume role '%v': %w", cfg.role, err)
		}
	}
	return pool, nil
}

func (f *Postgres) MustConnect(ctx context.Context, opts ...PostgresConnOpt) *pgxpool.Pool {
	pool, err := f.Connect(ctx, opts...)
	if err != nil {
		panic(err)
	}
	return pool
}

func (f *Postgres) HostName() string {
	return HostName(f.resource)
}

func (f *Postgres) Psql(ctx context.Context, cmd []string, mounts []string, quiet bool) (int, error) {
	// We're going to connect over the docker network
	settings := f.settings.Copy()
	settings.Host = HostIP(f.resource, f.docker.Network())
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
			f.docker.Network(),
		},
		Cmd: cmd,
	}
	// f.log.Debug("psql setup", zap.Any("environment", opts.Env))
	resource, err := f.docker.Pool().RunWithOptions(&opts)
	if err != nil {
		return 0, err
	}
	exitCode, err := WaitForContainer(f.docker.Pool(), resource)
	containerName := resource.Container.Name[1:]
	containerID := resource.Container.ID[0:11]
	if err != nil || exitCode != 0 && !quiet {
		f.log.Debug("psql failed", zap.Int("status", exitCode), zap.String("container_name", containerName), zap.String("container_id", containerID), zap.String("cmd", strings.Join(cmd, " ")))
		return exitCode, fmt.Errorf("psql exited with error (%v): %v", exitCode, getLogs(f.log, containerID, f.docker.Pool()))
	}
	if f.skipTearDown && getEnv().Debug {
		// If there was an issue, and debug is enabled, don't destroy the container.
		return exitCode, nil
	}
	f.docker.Purge(resource)
	return exitCode, nil
}

func (f *Postgres) PingPsql(ctx context.Context) error {
	_, err := f.Psql(ctx, []string{"psql", "-c", ";"}, []string{}, false)
	return err
}

func (f *Postgres) Ping(ctx context.Context) error {
	db, err := f.Connect(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Ping(ctx)
}

func (f *Postgres) CreateDatabase(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("must provide a database name")
	}
	exitCode, err := f.Psql(ctx, []string{"createdb", "--template=template0", name}, []string{}, false)
	f.log.Debug("create database", zap.Int("status", exitCode), zap.String("database", name), zap.String("container", f.HostName()))
	return err
}

// CopyDatabase creates a copy of an existing postgres database using `createdb --template={source} {target}`
// source will default to the primary database
func (f *Postgres) CopyDatabase(ctx context.Context, source string, target string) error {
	if source == "" {
		source = f.settings.Database
	}
	exitCode, err := f.Psql(ctx, []string{"createdb", fmt.Sprintf("--template=%v", source), target}, []string{}, false)
	f.log.Debug("copy database", zap.Int("status", exitCode), zap.String("source", source), zap.String("target", target), zap.String("container", f.HostName()))
	return err
}

func (f *Postgres) DropDatabase(ctx context.Context, name string) error {
	db, err := f.Connect(ctx, PostgresConnDatabase(name))
	if err != nil {
		return err
	}
	defer db.Close()

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
	f.log.Debug("drop database", zap.Int("status", exitCode), zap.String("database", name), zap.String("container", f.HostName()))
	return err
}

func (f *Postgres) Dump(ctx context.Context, dir string, filename string) error {
	path := FindPath(dir)
	if path == "" {
		return fmt.Errorf("could not resolve path: %v", dir)
	}
	exitCode, err := f.Psql(ctx, []string{"sh", "-c", fmt.Sprintf("pg_dump -Fc -Z0 %v > /tmp/%v", f.settings.Database, filename)}, []string{fmt.Sprintf("%v:/tmp", path)}, false)
	f.log.Debug("dump database", zap.Int("status", exitCode), zap.String("database", f.settings.Database), zap.String("container", f.HostName()), zap.String("path", path))
	return err
}

func (f *Postgres) Restore(ctx context.Context, dir string, filename string) error {
	path := FindPath(dir)
	if path == "" {
		return fmt.Errorf("could not resolve path: %v", dir)
	}
	exitCode, err := f.Psql(ctx, []string{"sh", "-c", fmt.Sprintf("pg_restore --dbname=%v --verbose --single-transaction /tmp/%v", f.settings.Database, filename)}, []string{fmt.Sprintf("%v:/tmp", path)}, false)
	f.log.Debug("restore database", zap.Int("status", exitCode), zap.String("database", f.settings.Database), zap.String("container", f.HostName()), zap.String("path", path))
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
		f.log.Debug("load sql", zap.Int("status", exitCode), zap.String("database", f.settings.Database), zap.String("container", f.HostName()), zap.String("name", name))
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

		port := ContainerTcpPort(f.resource, f.docker.Network(), "5432")
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
	db, err := f.Connect(ctx, PostgresConnDatabase(database))
	if err != nil {
		return false, err
	}
	defer db.Close()
	query := "SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename = $2"
	count := 0
	if err := db.QueryRow(ctx, query, schema, table).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func (f *Postgres) TableColumns(ctx context.Context, database, schema, table string) ([]string, error) {
	db, err := f.Connect(ctx, PostgresConnDatabase(database))
	if err != nil {
		return nil, err
	}
	defer db.Close()
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

func (f *Postgres) Tables(ctx context.Context, database string) ([]string, error) {
	db, err := f.Connect(ctx, PostgresConnDatabase(database))
	if err != nil {
		return nil, err
	}
	defer db.Close()
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

type model interface {
	TableName() string
}

func (f *Postgres) ValidateModels(ctx context.Context, databaseName string, i ...interface{}) error {
	for _, iface := range i {
		if err := f.ValidateModel(ctx, databaseName, iface); err != nil {
			return err
		}
	}
	return nil
}

func (f *Postgres) ValidateModel(ctx context.Context, databaseName string, i interface{}) error {
	var tableName string
	switch v := i.(type) {
	case model:
		tableName = strings.Trim(v.TableName(), "\"")
	default:
		tableName = strcase.ToSnake(structs.Name(v))
	}

	var schemaName string = "public"
	if s, t, found := strings.Cut(tableName, "."); found {
		schemaName = strings.Trim(s, "\"")
		tableName = strings.Trim(t, "\"")
	}

	exists, err := f.TableExists(ctx, databaseName, schemaName, tableName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("table %v.%v does not exist", schemaName, tableName)
	}

	fieldNames := columns(i)
	columnNames, err := f.TableColumns(ctx, databaseName, schemaName, tableName)
	if err != nil {
		return err
	}

	for _, f := range fieldNames {
		found := false
		for _, c := range columnNames {
			if f == c {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("struct %v contains field %v which does not exist in table: %v.%v{%v}", structs.Name(i), f, schemaName, tableName, columnNames)
		}
	}
	return nil
}

// Given a struct, return the expected column names.
func columns(i interface{}) []string {
	sf := structs.Fields(i)
	fields := []string{}
	for _, f := range sf {
		if tag := f.Tag.Get("db"); tag == "-" {
			continue
		} else if tag == "" {
			fields = append(fields, strcase.ToSnake(f.Name))
		} else {
			fields = append(fields, tag)
		}
	}
	return fields
}

// Deprecated: use Settings()
func (f *Postgres) GetSettings() *ConnectionSettings {
	return f.settings
}

// Deprecated: use ConnConfig()
func (f *Postgres) GetConnConfig() (*pgxpool.Config, error) {
	return pgxpool.ParseConfig(f.settings.String())
}

// Deprecated: use Connect(ctx, PostgresConnDatabase("database_name"))
func (f *Postgres) GetConnection(ctx context.Context, database string) (*pgx.Conn, error) {
	settings := f.settings.Copy()
	if database != "" {
		settings.Database = database
	}
	return settings.Connect(ctx)
}

// Deprecated: use HostName()
func (f *Postgres) GetHostName() string {
	return f.HostName()
}

// Deprecated: use Tables()
func (f *Postgres) GetTables(ctx context.Context, database string) ([]string, error) {
	return f.Tables(ctx, database)
}

// Deprecated: use TableColumns()
func (f *Postgres) GetTableColumns(ctx context.Context, database, schema, table string) ([]string, error) {
	return f.TableColumns(ctx, database, schema, table)
}
