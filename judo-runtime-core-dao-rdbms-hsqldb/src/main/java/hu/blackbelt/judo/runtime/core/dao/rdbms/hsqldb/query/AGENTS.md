# AGENTS.md — `judo-runtime-core-dao-rdbms-hsqldb/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/hsqldb/query`

HSQLDB-specific overrides of the generic query-parameter binding.

| File | Purpose |
| --- | --- |
| `HsqldbRdbmsParameterMapper.java` | Extends `DefaultRdbmsParameterMapper` and implements `RdbmsParameterMapper` for HSQLDB. Its `@Builder` constructor takes `@NonNull` `Coercer`, `RdbmsModel`, `IdentifierProvider`, then rewrites two entries of `getSqlTypes()`: `Time.class` binds as `TIMESTAMP` and `String.class` as `LONGVARCHAR`. Overrides happen in the constructor, so a caller mutating `getSqlTypes()` afterwards wins. |
