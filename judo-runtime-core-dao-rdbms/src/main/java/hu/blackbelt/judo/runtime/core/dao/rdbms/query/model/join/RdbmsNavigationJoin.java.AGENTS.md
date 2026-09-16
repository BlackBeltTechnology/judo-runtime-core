# `RdbmsNavigationJoin.java`

Renders a navigation `SubSelect` as a `SELECT DISTINCT` derived table joined into its container: `@Builder` ctor takes `@NonNull SubSelect query`, `RdbmsBuilderContext builderContext`, `boolean withoutFeatures`, expands navigation joins via `rdbmsBuilder.processJoin(JoinProcessParameters...)`, collects `subJoins`/`subFeatures`/`subConditions`, sets `alias = RdbmsAliasUtil.AGGREGATE_PREFIX + query.getAlias()`, and flags `aggregatedNavigation`.

**Key exports** `getLastJoin()` (deepest join of `query.getJoins()`), `getTableNameOrSubQuery(SqlConverterContext)` emitting `(SELECT DISTINCT … FROM <subFrom> … WHERE … GROUP BY …)` with `GROUP BY` gated on `aggregatedNavigation`, `@Getter List<RdbmsOrderBy> exposedOrderBys`.

**Contracts** ctor `checkArgument`s null base / empty navigation joins; only the last navigation join's orderBys reach `exposedOrderBys`; `subFeatures` carries base id column under `RdbmsAliasUtil.getParentIdColumnAlias(query.getContainer())`.