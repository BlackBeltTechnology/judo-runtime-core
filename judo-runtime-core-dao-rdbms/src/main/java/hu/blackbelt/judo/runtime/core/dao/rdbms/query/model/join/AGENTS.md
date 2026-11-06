# AGENTS.md — `judo-runtime-core-dao-rdbms/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/query/model/join`

JOIN fragments of the RDBMS query layer. Each subtype answers one question —
"what do I join against?" (`getTableNameOrSubQuery`) — and optionally overrides the ON
condition; `RdbmsJoin.toSql` wraps both into `JOIN`/`LEFT OUTER JOIN` text. Emission order
is decided by `RdbmsJoinComparator`, not by insertion order.

| File | Purpose |
| --- | --- |
| `RdbmsContainerJoin.java` | Joins back to the composition container across several `EReference`s; `@SuperBuilder`, exports `POSTFIX = "_c"`, `COALESCE` partner matching per reference index, `checkArgument` on an empty reference list. → see `RdbmsContainerJoin.java.AGENTS.md` |
| `RdbmsCustomJoin.java` | Wraps a caller-supplied SQL snippet as a derived table: `getTableNameOrSubQuery` returns `"(" + sql + ")"`. `@SuperBuilder` over `@NonNull String sql` and `@NonNull String sourceIdSetParameterName`. The SQL is embedded verbatim — the custom query must bind `sourceIdSetParameterName` itself, and the base `getJoinCondition` still supplies the ON clause. |
| `RdbmsJoin.java` | Abstract base of every join fragment; owns `toSql`, the default ON-condition builder, junction-table handling and alias bookkeeping. → see `RdbmsJoin.java.AGENTS.md` |
| `RdbmsJoinComparator.java` | Orders joins after their dependencies: prefers `aliasToCompareWith`, pivots on `joinConditionTableAliases`, falls back to `originalOrder.indexOf`; throws `IllegalArgumentException` on null/blank alias. → see `RdbmsJoinComparator.java.AGENTS.md` |
| `RdbmsNavigationJoin.java` | Renders a navigation `SubSelect` as a `SELECT DISTINCT` derived table joined to its container, carrying order-by exposure and aggregation grouping. → see `RdbmsNavigationJoin.java.AGENTS.md` |
| `RdbmsQueryJoin.java` | Joins against a nested `RdbmsResultSet` rendered as a parenthesised subquery. `@SuperBuilder` over `@NonNull RdbmsResultSet resultSet`; `getTableNameOrSubQuery` renders it with `includeAlias(true)` (the subquery must expose named columns) and merges the result set's `joinConditionTableAliases` into its own so the comparator sees the transitive dependencies. |
| `RdbmsTableJoin.java` | Joins a plain physical table; `@SuperBuilder` over `tableName` + optional `rdbmsPartnerTable` that wins over inherited `partnerTable`, else delegates to `RdbmsJoin.getJoinCondition`. → see `RdbmsTableJoin.java.AGENTS.md` |
