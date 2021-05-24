# Postgres

This packge offers easy access to postgres and psql from golang for testing purposes.

##### Fixtures

| Name               | Description                                                  |
| ------------------ | ------------------------------------------------------------ |
| Postgres           | Spin up a postgres database. Connect with `fixture.GetConnection()`. *Will wait for the database to be ready.* |
| PostgresWithSchema | Same as Postgres, but also loads sql files during setup via psql. Configure using attribute `FileGlob`. |
| Psql               | Run `psql` in docker against a postgres container. Call directly, or spin up a postgres container and call `postgres_fixture.Psql()`. |

## Example

Basic intro to using this library with tests that require loading a dump of an SQL schema.

In an ideal world, run migrations against the database instead.

In this example, *postgres is only run once per package*, and the database is duplicated for each individual test to ensure an absolutely clean environment for each subsequent test.

### Step 1: Create Database Fixture

Call this bad boy from `TestMain()`.

```go
import (
	"github.com/jmoiron/sqlx"
	"github.com/charlieparkes/ezsqlx"
	"github.com/charlieparkes/go-fixtures"
)

func GetDatabaseFixture() *fixtures.Fixtures {
	f := &fixtures.Fixtures{}
	docker := &fixtures.Docker{}
  f.Add(docker)
	db := &fixtures.PostgresWithSchema{
		Docker: &docker,
    FileGlob: "./migrations/*.sql",
	}
  f.AddByName("db_name", db)
	return f
}
```

### Step 2: For idempotentcy, copy the schema!

```go
func CopySchema(f *fixtures.Fixtures) (*sqlx.DB, error) {
	databaseFixture := f.Get("db_name").(*fixtures.PostgresWithSchema)
	tmpdb := &fixtures.PostgresDatabaseCopy{
		Postgres: databaseFixture.Postgres.Postgres,
	}
	err := tmpdb.SetUp()
	if err != nil {
		return nil, err
	}
	db, err := tmpdb.GetConnection()
  if err != nil {
    return nil, err
  }
	return db, nil
}
```

### Step 3: Write some tests!

```go
import (
	"os"
  
	"yourproject/test"
  
	go_fixtures "github.com/charlieparkes/go-fixtures"
)

var fixtures *go_fixtures.Fixtures

func TestMain(m *testing.M) {
	fixtures = test.GetDatabaseFixture()
	status := m.Run()
	fixtures.TearDown()
	os.Exit(status)
}

func TestThing(t *testing.T) {
	db, err := test.CopySchema(fixtures)
  assert.NoErr(t, err)
	db.Ping()
}
```

## Debugging Postgres in Docker

1. Disable teardown in your individual test and your package's `TestMain`.
2. Once this is done, open a shell using `ctop` or `docker exec -it bash CONTAINER_ID`.
3. Become the postgres user. `su postgres`
4. Open a postgres shell. `psql`
5. List the available tables. `\l`
6. Connect to the test's duplicate table. `\c {table_name}`
7. You're good to go! For example, list the tables in the current schema. `\dt`
