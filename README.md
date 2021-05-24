# go-fixtures

Why mock when you can just run the damn thing?

Inspired by [pytest](https://github.com/pytest-dev/pytest), go-fixtures provides a collection of fixtures which automagically setup/teardown services.

### Supported Fixtures
* docker: *`Docker`*
* [postgres](./examples/postgres): *`Postgres`, `PostgresWithSchema`, `Psql`*
* aws (localstack): coming soon