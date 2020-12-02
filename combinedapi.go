package fixtures

import (
	"github.com/jmoiron/sqlx"
	"github.com/tovala/ezsqlx"
)

type CombinedAPIDatabase struct {
	BaseFixture
	Network *DockerNetwork

	Postgres *PostgresWithSchema
}

func (f *CombinedAPIDatabase) SetUp() error {
	settings := &ezsqlx.ConnectionSettings{
		Host:         "localhost",
		User:         "postgres",
		Password:     generateString(),
		Database:     "combinedapi",
		DisableSSL:   true,
		MaxOpenConns: 50,
	}
	f.Postgres = &PostgresWithSchema{
		Pool:     f.Network.Pool,
		Network:  f.Network,
		Settings: settings,
		Version:  "9.6",
	}
	return f.Postgres.SetUp()
}

func (f *CombinedAPIDatabase) TearDown() error {
	return f.Postgres.TearDown()
}

func (f *CombinedAPIDatabase) GetConnection() (*sqlx.DB, func() error) {
	return f.Postgres.GetConnection()
}
