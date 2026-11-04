# AGENTS.md — `judo-runtime-core-dao-rdbms/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/query/utils`

| File | Purpose |
| --- | --- |
| `RdbmsAliasUtil.java` | Static alias-name builder for generated SQL. `getTargetColumnAlias(target, alias)` returns `alias + "_" + target.getIndex()`; `getInstanceIdsKey`/`getParentIdsKey`, `getNavigationSubSelectAlias`, `AGGREGATE_PREFIX = "aggr_"`. → see `RdbmsAliasUtil.java.AGENTS.md` |
