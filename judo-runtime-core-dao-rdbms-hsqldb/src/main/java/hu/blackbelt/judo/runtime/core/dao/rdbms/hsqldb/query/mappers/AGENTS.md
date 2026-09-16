# AGENTS.md — `judo-runtime-core-dao-rdbms-hsqldb/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/hsqldb/query/mappers`

HSQLDB-dialect SQL patterns for query functions, wired into the generic mapper factory.

| File | Purpose |
| --- | --- |
| `HsqldbFunctionMapper.java` | Registers HSQLDB SQL patterns per `FunctionSignature` in `getFunctionBuilderMap()`. `@Builder` ctor takes `@NonNull RdbmsBuilder`; LIKE/ILIKE render CASE/WHEN with NULL passthrough, time parts use `EXTRACT`/`TO_TIMESTAMP`/`UNIX_MILLIS`/`TO_CHAR`, arithmetic uses `DATEADD`/`TIMESTAMPADD`; `TIME_FROM_MILLISECONDS` reuses the `TIMESTAMP_FROM_MILLISECONDS` pattern. |
| `HsqldbMapperFactory.java` | Extends `DefaultMapperFactory`; `getMappers(RdbmsBuilder)` returns super's mapper map plus `Function.class` bound to `HsqldbFunctionMapper`. Callers mutating the returned map override the HSQLDB function mapping. |