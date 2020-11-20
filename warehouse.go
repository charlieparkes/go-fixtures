package fixtures

import (
	"github.com/jmoiron/sqlx"
	"github.com/tovala/ezsqlx"
)

type WarehouseDatabase struct {
	BaseFixture
	Network *DockerNetwork

	Postgres *PostgresWithSchema
}

func (f *WarehouseDatabase) SetUp() error {
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
	f.Postgres.SetUp()
	return nil
}

func (f *WarehouseDatabase) TearDown() error {
	return f.Postgres.TearDown()
}

func (f *WarehouseDatabase) GetConnection() (*sqlx.DB, func() error) {
	return f.Postgres.GetConnection()
}