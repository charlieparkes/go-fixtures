package fixtures

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPostgres(t *testing.T) {
	start := time.Now()
	fixtures := Fixtures{}
	defer fixtures.TearDown()

	d := &Docker{NamePrefix: "gofixtures"}
	fixtures.AddByName("docker", d)
	fmt.Printf("Setup docker: %v\n", time.Since(start))
	start = time.Now()

	p := &PostgresWithSchema{
		Docker:   d,
		PathGlob: "./testdata/migrations/*.sql",
	}
	fixtures.AddByName("postgres", p)
	fmt.Printf("Setup postgres: %v\n", time.Since(start))
	start = time.Now()

	db, err := p.GetConnection()
	assert.NoError(t, err)

	tables := []string{}
	assert.NoError(t, db.Select(&tables, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'information_schema' AND schemaname != 'pg_catalog';"))
	assert.Len(t, tables, 2) // migrations define two tables
	assert.NoError(t, db.Close())
	fmt.Printf("Tested migrations: %v\n", time.Since(start))
	start = time.Now()

	c := &PostgresDatabaseCopy{
		Postgres: p.Postgres,
	}
	fixtures.AddByName("db_copy", c)
	fmt.Printf("Setup database copy: %v\n", time.Since(start))
	start = time.Now()

	db, err = c.GetConnection()
	assert.NoError(t, err)

	tables = []string{}
	assert.NoError(t, db.Select(&tables, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'information_schema' AND schemaname != 'pg_catalog';"))
	assert.Len(t, tables, 2)
	fmt.Printf("Tested migrations: %v\n", time.Since(start))
	start = time.Now()
}
