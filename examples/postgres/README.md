# Postgres

This packge provides easy access to postgres and psql from code for testing purposes.

## Example

See `example_test.go`.

In this example, *postgres is only run once per package*, and the database is cloned for each individual test to ensure idempotency.


## Debugging Postgres in Docker

1. Disable teardown in your individual test and your package's `TestMain`.
2. Once this is done, open a shell using `ctop` or `docker exec -it bash CONTAINER_ID`.
3. Become the postgres user. `su postgres`
4. Open a postgres shell. `psql`
5. List the available tables. `\l`
6. Connect to the test's duplicate table. `\c {table_name}`
7. You're good to go! For example, list the tables in the current schema. `\dt`
