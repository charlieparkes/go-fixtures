package fixtures

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
)

type ConnectionSettings struct {
	Driver       string
	Host         string
	Port         string
	User         string
	Password     string
	Database     string
	DisableSSL   bool
	MaxOpenConns int
}

func (cs *ConnectionSettings) String() string {
	sslmode := "require"
	if cs.DisableSSL {
		sslmode = "disable"
	}
	return fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v sslmode=%v",
		cs.Host,
		cs.Port,
		cs.User,
		cs.Password,
		cs.Database,
		sslmode,
	)
}

func (cs *ConnectionSettings) Copy() *ConnectionSettings {
	s := *cs
	return &s
}

func (cs *ConnectionSettings) Connect(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, cs.String())
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}
	return conn, nil
}
