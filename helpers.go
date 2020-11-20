package fixtures

import (
	"log"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest/v3"
)

func GetDatabaseFixtures() *Fixtures {
	fixtures := &Fixtures{}

	pool := DockerPool{}
	fixtures.Add(&pool)

	network := DockerNetwork{
		Pool:       &pool,
		NamePrefix: "oatmeal_test",
	}
	fixtures.Add(&network)

	db := CombinedAPIDatabase{
		Network: &network,
	}
	fixtures.AddByName("combinedapi", &db)

	return fixtures
}

func LoadCombinedAPISchema(fixtures *Fixtures) (*sqlx.DB, func()) {
	databaseFixture := fixtures.Get("combinedapi").(*CombinedAPIDatabase)
	tmpdb := &PostgresDatabaseCopy{
		Postgres: databaseFixture.Postgres.Postgres,
	}
	err := tmpdb.SetUp()
	if err != nil {
		log.Fatalf("Failed to setup CombinedAPI with schema.: %v", err)
	}
	db, closeHandler := tmpdb.GetConnection()
	tearDown := func() {
		closeHandler()
		tmpdb.TearDown()
	}
	return db, tearDown
}

func generateString() string {
	rand.Seed(time.Now().UTC().UnixNano())
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, 10) // Make some space
	for i := 0; i < 10; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

func getTestDataPath(name string) string {
	path := filepath.Join("testdata", name)
	return path
}

func waitForContainer(pool *dockertest.Pool, resource *dockertest.Resource) int {
	exitCode, err := pool.Client.WaitContainer(resource.Container.ID)
	if err != nil {
		log.Fatalf("Unable to wait for container: %s", err)
	}
	return exitCode
}