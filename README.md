# go-fixtures

Inspired by [pytest](https://github.com/pytest-dev/pytest), go-fixtures provides a collection of fixtures which automagically setup/teardown services I frequently find myself testing against.

### Supported Fixtures
* [docker](./pkg/docker): *`Docker`*
* [postgres](./pkg/postgres): *`Postgres`, `PostgresWithSchema`, `Psql`*
* [aws (localstack)](./pkg/aws) coming soon