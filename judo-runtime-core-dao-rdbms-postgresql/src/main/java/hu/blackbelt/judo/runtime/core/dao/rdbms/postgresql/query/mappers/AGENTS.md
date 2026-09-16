# AGENTS.md — `judo-runtime-core-dao-rdbms-postgresql/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/postgresql/query/mappers`

PostgreSQL-specific renderers for query-model functions and the mapper factory that wires them into the RDBMS query pipeline.

| File | Purpose |
| --- | --- |
| `PostgresqlFunctionMapper.java` | Renders query `FunctionSignature`s as PostgreSQL SQL. `@Builder` ctor takes `@NonNull RdbmsBuilder`, fills `getFunctionBuilderMap()`: modulo as `%`, string casts via `CAST(... AS TEXT)`/`TO_CHAR` CASE, regex match `~`, `LPAD`/`RPAD`, `INTERVAL` add/diff arithmetic, `TO_TIMESTAMP` ms round-trips, `EXTRACT(DOW|DOY)` with DOW 0→7. |
| `PostgresqlMapperFactory.java` | Extends `DefaultMapperFactory`; overrides `getMappers(RdbmsBuilder)` to inject `Function.class` → `PostgresqlFunctionMapper` into the mapper map returned by super. |