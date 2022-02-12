package fixtures

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgres(t *testing.T) {
	ctx := context.Background()
	fixtures := Fixtures{}
	defer fixtures.RecoverTearDown(ctx)

	dockerOpts := []DockerOpt{
		DockerOptNamePrefix("gofixtures"),
	}
	if networkName := os.Getenv("HOST_NETWORK_NAME"); networkName != "" {
		dockerOpts = append(dockerOpts, DockerOptNetworkName(networkName))
	}
	d := NewDocker(dockerOpts...)
	t.Run("Docker", func(t *testing.T) {
		require.NoError(t, fixtures.AddByName(ctx, "docker", d))
	})

	var p1 *Postgres
	t.Run("Postgres", func(t *testing.T) {
		p1 = NewPostgres(d)
		require.NoError(t, fixtures.Add(ctx, p1))
	})

	t.Run("TableExists_False", func(t *testing.T) {
		exists, err := p1.TableExists(ctx, "", "public", "address")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("LoadSqlPattern", func(t *testing.T) {
		require.NoError(t, p1.LoadSqlPattern(ctx, "./testdata/migrations/*.sql"))

		db, err := p1.GetConnection(ctx, "")
		assert.NoError(t, err)
		tables, err := p1.GetTables(ctx, "")
		require.NoError(t, err)
		assert.Len(t, tables, 2)
		_ = db.Close(ctx)
	})

	t.Run("TableExists_True", func(t *testing.T) {
		exists, err := p1.TableExists(ctx, "", "public", "address")
		assert.NoError(t, err)
		assert.True(t, exists)
		if !exists {
			tables, err := p1.GetTables(ctx, "")
			require.NoError(t, err)
			fmt.Println(tables)
		}
	})

	t.Run("Dump", func(t *testing.T) {
		require.NoError(t, p1.Dump(ctx, "testdata/tmp", "test.pgdump"))
	})

	t.Run("CreateDatabase", func(t *testing.T) {
		name := namesgenerator.GetRandomName(0)
		require.NoError(t, p1.CreateDatabase(ctx, name))
		db, err := p1.GetConnection(ctx, name)
		assert.NoError(t, err)
		_ = db.Close(ctx)
	})

	databaseName := namesgenerator.GetRandomName(0)
	t.Run("CopyDatabase", func(t *testing.T) {
		require.NoError(t, p1.CopyDatabase(ctx, "", databaseName))

		db, err := p1.GetConnection(ctx, databaseName)
		assert.NoError(t, err)
		assert.NoError(t, db.Close(ctx))

		tables, err := p1.GetTables(ctx, databaseName)
		require.NoError(t, err)
		assert.Len(t, tables, 2)

		// Original.
		exists, err := p1.TableExists(ctx, "", "public", "address")
		assert.NoError(t, err)
		assert.True(t, exists)
		// New copy.
		exists, err = p1.TableExists(ctx, databaseName, "public", "address")
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("Restore", func(t *testing.T) {
		p2 := NewPostgres(d)
		require.NoError(t, fixtures.Add(ctx, p2))

		assert.NoError(t, p2.Restore(ctx, "testdata/tmp", "test.pgdump"))

		db, err := p2.GetConnection(ctx, "")
		assert.NoError(t, err)
		assert.NoError(t, db.Close(ctx))

		tables, err := p2.GetTables(ctx, "")
		require.NoError(t, err)
		assert.Len(t, tables, 2)

		exists, err := p2.TableExists(ctx, "", "public", "address")
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("Teardown", func(t *testing.T) {
		require.NoError(t, fixtures.TearDown(ctx))
	})
}
