package postgres

import (
	"context"
	_ "embed"
	"log"
	"os"
	"testing"

	"github.com/charlieparkes/go-fixtures/v2"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var f *fixtures.Fixtures

func TestMain(m *testing.M) {
	ctx := context.Background()
	f = setup(ctx)
	defer f.RecoverTearDown(ctx)
	status := m.Run()
	teardown(ctx, f)
	os.Exit(status)
}

func TestExample(t *testing.T) {
	ctx := context.Background()

	pool, err := f.Postgres().Connect(ctx)
	require.NoError(t, err)
	defer pool.Close()

	database := pool.Config().ConnConfig.Database

	exists, err := f.Postgres().TableExists(ctx, database, "public", "example")
	assert.NoError(t, err)
	assert.True(t, exists)
}

// ---- example migrations ------------

//go:embed migration.sql
var migration string

func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, migration); err != nil {
		panic(err)
	}
	return nil
}

// ---- example test/test.go ------------
func setup(ctx context.Context) *fixtures.Fixtures {
	f := fixtures.NewFixtures()
	defer f.RecoverTearDown(ctx)

	f.Add(ctx, fixtures.NewDocker())
	f.Add(ctx, fixtures.NewPostgres(f.Docker(), fixtures.PostgresSkipTearDown()))

	pool := f.Postgres().MustConnect(ctx)
	defer pool.Close()
	if err := applyMigrations(ctx, pool); err != nil {
		panic(err)
	}
	return f
}

func teardown(ctx context.Context, f *fixtures.Fixtures) {
	if f == nil {
		return
	}
	if err := f.TearDown(ctx); err != nil {
		log.Println("failed to tear down:", err)
	}
}
