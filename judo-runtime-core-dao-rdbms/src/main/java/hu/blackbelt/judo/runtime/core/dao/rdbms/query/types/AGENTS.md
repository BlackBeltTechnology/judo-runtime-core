# AGENTS.md — `judo-runtime-core-dao-rdbms/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/query/types`

SQL type value objects used by the RDBMS query builder when rendering typed columns — currently
one DSL type, `RdbmsDecimalType`.

| File | Purpose |
| --- | --- |
| `RdbmsDecimalType.java` | Models SQL `DECIMAL(p,s)` value object. Exports `DEFAULT_PRECISION`, `getPrecision()`, `getScale()`, `toSql()`; ctor validates precision/scale bounds. → see `RdbmsDecimalType.java.AGENTS.md` |