# AGENTS.md — `judo-runtime-core-dao-rdbms-hsqldb/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/hsqldb`

HSQLDB binding of the generic RDBMS DAO: dialect identity, schema bootstrap, and
sequence access.

| File | Purpose |
| --- | --- |
| `HsqldbDialect.java` | Implements `Dialect` for HSQLDB. `getName()` returns the literal `"hsqldb"` used to select dialect-specific mappers and SQL fragments; `getDualTable()` returns `"INFORMATION_SCHEMA"."SYSTEM_USERS"` as the from-less select target, so generated SQL must always qualify a dual table rather than omit `FROM`. |
| `HsqldbRdbmsInit.java` | Implements `RdbmsInit` by delegating `execute(DataSource)` to `SimpleLiquibaseExecutor.createDatabase(dataSource, liquibaseModel)`. Lombok `@Builder` requires both `@NonNull liquibaseExecutor` and `@NonNull liquibaseModel`; the model is fixed at construction, so one instance bootstraps exactly one schema. |
| `HsqldbRdbmsSequence.java` | Implements `Sequence<Long>` over HSQLDB `NEXT VALUE FOR` / `CURRENT VALUE FOR` via `NamedParameterJdbcTemplate` → see `HsqldbRdbmsSequence.java.AGENTS.md` |
