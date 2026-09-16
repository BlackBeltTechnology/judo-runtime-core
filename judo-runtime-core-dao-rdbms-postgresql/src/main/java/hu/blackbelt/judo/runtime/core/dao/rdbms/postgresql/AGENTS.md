# AGENTS.md — `judo-runtime-core-dao-rdbms-postgresql/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/postgresql`

PostgreSQL wiring: dialect identification, parameter-type mapping, schema init, and connectivity probing.

| File | Purpose |
| --- | --- |
| `PostgresqlConnectionTester.java` | Probes PostgreSQL reachability. Static `testConnection(String url, String user, String password)` instantiates `org.postgresql.Driver`, opens and closes a `DriverManager` connection; any failure surfaces as `ConnectException` wrapping the driver message. |
| `PostgresqlDialect.java` | Implements `Dialect` for PostgreSQL. `getName()` returns `postgresql`; `getDualTable()` returns `null` (no dual table in PostgreSQL). |
| `PostgresqlRdbmsInit.java` | Implements `RdbmsInit`; `@Builder` ctor takes `@NonNull SimpleLiquibaseExecutor` and `@NonNull LiquibaseModel`. `execute(DataSource)` runs the fixed `liquibase/postgresql-init-changelog.xml` from `PostgresqlRdbmsInit`'s classloader, then `createDatabase(dataSource, liquibaseModel)`. |
| `PostgresqlRdbmsParameterMapper.java` | Extends `DefaultRdbmsParameterMapper` implementing `RdbmsParameterMapper`. Private `@Builder` ctor takes `@NonNull Coercer`, `RdbmsModel`, `IdentifierProvider` and rewrites `getSqlTypes()`: `String.class` binds as `TEXT`, `Double.class` as `DOUBLE PRECISION`. Overrides happen in the ctor, so a caller mutating `getSqlTypes()` afterwards wins. |