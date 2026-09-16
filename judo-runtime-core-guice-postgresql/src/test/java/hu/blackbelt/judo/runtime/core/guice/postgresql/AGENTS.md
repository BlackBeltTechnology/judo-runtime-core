# AGENTS.md — `judo-runtime-core-guice-postgresql/src/test/java/hu/blackbelt/judo/runtime/core/guice/postgresql`

Package-level JUnit tests wiring the PostgreSQL-backed guice module.

| File | Purpose |
| --- | --- |
| `JudoDefaultPostgresqlModuleTest.java` | JUnit 5 smoke test booting `JudoDefaultModule` + `JudoPostgresqlModule` against a Testcontainers `PostgreSQLContainer("postgres:16-alpine")`: picks an ephemeral host port via `ServerSocket(0)`, maps it to container port 5432, feeds container host/user/password/port into the module. `test()` asserts `injector.getInstance(DAO.class)`; `teardownDatasource()` stops the container when still running. |