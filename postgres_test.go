package fixtures

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgres(t *testing.T) {
	d := &Docker{NamePrefix: "gofixtures"}
	p := &Postgres{
		Docker: d,
	}
	fixtures := Fixtures{}
	defer fixtures.TearDown()
	fixtures.Add(d, p)
}

func TestPostgresWithSchema(t *testing.T) {
	d := &Docker{NamePrefix: "gofixtures"}
	p := &PostgresWithSchema{
		Docker:   d,
		PathGlob: "./testdata/migrations/*.sql",
	}
	fixtures := Fixtures{}
	defer fixtures.TearDown()
	fixtures.Add(d, p)

	db, err := p.GetConnection()
	assert.NoError(t, err)

	tables := []string{}
	assert.NoError(t, db.Select(&tables, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'information_schema' AND schemaname != 'pg_catalog';"))
	assert.Len(t, tables, 2) // migrations define two tables
}
