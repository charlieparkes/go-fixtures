package fixtures

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgres(t *testing.T) {
	ctx := context.Background()
	timer := newTimer()
	fixtures := Fixtures{}
	defer fixtures.TearDown(ctx)

	d := &Docker{NamePrefix: "gofixtures"}
	fixtures.AddByName(ctx, "docker", d)
	timer.PrintSplit("Docker")

	p1 := &PostgresWithSchema{
		Docker:   d,
		PathGlob: "./testdata/migrations/*.sql",
	}
	fixtures.AddByName(ctx, "postgres_with_schema", p1)
	timer.PrintSplit("PostgresWithSchema")

	db, err := p1.GetConnection(ctx)
	assert.NoError(t, err)

	assert.Len(t, getTables(t, ctx, db), 2) // migrations define two tables
	assert.NoError(t, db.Close(ctx))
	timer.PrintSplit("Migrations check")

	p1.Postgres.DumpDatabase(ctx, "testdata/tmp", "test.pgdump")
	timer.PrintSplit("Postgres.DumpDatabase")

	c := &PostgresDatabaseCopy{
		Postgres:     p1.Postgres,
		SkipTearDown: true,
	}
	fixtures.AddByName(ctx, "db_copy", c)
	timer.PrintSplit("PostgresDatabaseCopy")

	db, err = c.GetConnection(ctx)
	assert.NoError(t, err)

	assert.Len(t, getTables(t, ctx, db), 2)
	timer.PrintSplit("Migrations check")

	p2 := &Postgres{
		Docker: d,
	}
	fixtures.AddByName(ctx, "postgres", p2)
	timer.PrintSplit("Postgres")

	err = p2.RestoreDatabase(ctx, "testdata/tmp", "test.pgdump")
	assert.NoError(t, err)
	timer.PrintSplit("Postgres.RestoreDatabase")

	db, err = p2.GetConnection(ctx, "")
	assert.NoError(t, err)

	assert.Len(t, getTables(t, ctx, db), 2)
	assert.NoError(t, db.Close(ctx))
	timer.PrintSplit("Migrations check")
}

func getTables(t *testing.T, ctx context.Context, db *pgx.Conn) []string {
	tables := []string{}
	rows, err := db.Query(ctx, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'information_schema' AND schemaname != 'pg_catalog'")
	require.NoError(t, err)
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		require.NoError(t, err)
		tables = append(tables, table)
	}
	return tables
}
