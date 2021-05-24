package postgres

import (
	"testing"

	"github.com/charlieparkes/go-fixtures/pkg/docker"
	"github.com/charlieparkes/go-fixtures/pkg/fixtures"
	"github.com/stretchr/testify/assert"
)

func TestPostgres(t *testing.T) {
	d := &docker.Docker{NamePrefix: "gofixtures"}
	p := &Postgres{
		Docker: d,
	}
	fixtures := fixtures.Fixtures{}
	fixtures.Add(d, p)
	assert.NoError(t, fixtures.TearDown())
}

func TestPostgresWithSchema(t *testing.T) {
	d := &docker.Docker{NamePrefix: "gofixtures"}
	p := &PostgresWithSchema{
		Docker:   d,
		PathGlob: "./testdata/*.sql",
	}
	fixtures := fixtures.Fixtures{}
	fixtures.Add(d, p)

	db, err := p.GetConnection()
	assert.NoError(t, err)

	tables := []string{}
	assert.NoError(t, db.Select(&tables, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'information_schema' AND schemaname != 'pg_catalog';"))
	assert.Len(t, tables, 2) // migrations define two tables

	assert.NoError(t, fixtures.TearDown())
}
