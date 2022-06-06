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
	fixtures := NewFixtures()
	defer fixtures.RecoverTearDown(ctx)

	dockerOpts := []DockerOpt{
		DockerNamePrefix("gofixtures"),
	}
	if networkName := os.Getenv("HOST_NETWORK_NAME"); networkName != "" {
		dockerOpts = append(dockerOpts, DockerNetworkName(networkName))
	}
	d := NewDocker(dockerOpts...)
	t.Run("Docker", func(t *testing.T) {
		require.NoError(t, fixtures.Add(ctx, d))
	})

	var p1 *Postgres
	t.Run("Create", func(t *testing.T) {
		p1 = NewPostgres(d)
		require.NoError(t, fixtures.Add(ctx, p1))
	})

	t.Run("Get", func(t *testing.T) {
		require.NotNil(t, fixtures.Postgres())
	})

	t.Run("Ping", func(t *testing.T) {
		require.NoError(t, p1.Ping(ctx))
		require.NoError(t, p1.PingPsql(ctx))
	})

	t.Run("TableExists_False", func(t *testing.T) {
		exists, err := p1.TableExists(ctx, "", "public", "address")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("LoadSqlPattern", func(t *testing.T) {
		require.NoError(t, p1.LoadSqlPattern(ctx, "./testdata/migrations/*.sql"))

		db, err := p1.Connect(ctx)
		require.NoError(t, err)
		if err == nil {
			db.Close()
		}
		tables, err := p1.GetTables(ctx, "")
		require.NoError(t, err)
		assert.Len(t, tables, 2)
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

	t.Run("ValidateModel", func(t *testing.T) {
		require.NoError(t, p1.ValidateModels(ctx, "", &Person{}))
	})

	t.Run("Dump", func(t *testing.T) {
		require.NoError(t, p1.Dump(ctx, "testdata/tmp", "test.pgdump"))
	})

	t.Run("CreateDatabase", func(t *testing.T) {
		name := namesgenerator.GetRandomName(0)
		require.NoError(t, p1.CreateDatabase(ctx, name))
		db, err := p1.Connect(ctx, PostgresConnDatabase(name))
		require.NoError(t, err)
		if err == nil {
			db.Close()
		}
	})

	t.Run("CopyDatabase", func(t *testing.T) {
		databaseName := namesgenerator.GetRandomName(0)
		require.NoError(t, p1.CopyDatabase(ctx, "", databaseName))

		db, err := p1.Connect(ctx, PostgresConnDatabase(databaseName))
		require.NoError(t, err)
		if err == nil {
			db.Close()
		}

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

	t.Run("ConnectCopyDatabase", func(t *testing.T) {
		db, err := p1.Connect(ctx, PostgresConnCreateCopy())
		require.NoError(t, err)
		if err == nil {
			db.Close()
		}

		tables, err := p1.GetTables(ctx, db.Config().ConnConfig.Database)
		require.NoError(t, err)
		assert.Len(t, tables, 2)

		// Original.
		exists, err := p1.TableExists(ctx, "", "public", "address")
		assert.NoError(t, err)
		assert.True(t, exists)
		// New copy.
		exists, err = p1.TableExists(ctx, db.Config().ConnConfig.Database, "public", "address")
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("Restore", func(t *testing.T) {
		p2 := NewPostgres(d)
		require.NoError(t, fixtures.Add(ctx, p2))

		assert.NoError(t, p2.Restore(ctx, "testdata/tmp", "test.pgdump"))

		db, err := p2.Connect(ctx)
		require.NoError(t, err)
		if err == nil {
			db.Close()
		}

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

type Person struct {
	Id        int64
	FirstName string
	LastName  string
	AddressId int64
	FooBar    bool `db:"-"`
}
