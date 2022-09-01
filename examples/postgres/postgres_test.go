package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/charlieparkes/go-fixtures/v2"
)

var f *fixtures.Fixtures

func TestMain(m *testing.M) {
	ctx := context.Background()
	f = fixtures.NewFixtures()
	f.Add(ctx, fixtures.NewDocker())
	f.Add(ctx, fixtures.NewPostgres(f.Docker()))
	defer f.RecoverTearDown(ctx)
	status := m.Run()
	f.TearDown(ctx)
	os.Exit(status)
}

func TestExample(t *testing.T) {
	ctx := context.Background()
	pool := f.Postgres().MustConnect(ctx)
	defer pool.Close()
}
