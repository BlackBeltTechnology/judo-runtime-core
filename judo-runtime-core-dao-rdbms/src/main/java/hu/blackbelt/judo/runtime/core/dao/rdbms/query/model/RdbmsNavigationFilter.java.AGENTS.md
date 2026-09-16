# `RdbmsNavigationFilter.java`

Turns a `hu.blackbelt.judo.meta.query.Filter` node into the correlated existence
subquery that a `WHERE EXISTS (…)` / join condition can embed: `SELECT 1 FROM <filter
table> … WHERE <partner>.ID = <filter>.ID AND …`.

**Key exports**

- `@Builder private RdbmsNavigationFilter(@NonNull Filter filter,
  @NonNull RdbmsBuilderContext builderContext)` — construction does all the work; the
  instance is immutable afterwards.
- `toSql(SqlConverterContext)` (override of `RdbmsField`).

**What the constructor assembles**

- `from` = `rdbmsBuilder.getTableName(filter.getType())`.
- `joins` — every join reachable from `filter.getJoins()` expanded through
  `getAllJoins()` and `rdbmsBuilder.processJoin(JoinProcessParameters … withoutFeatures(true))`.
- `conditions` — `rdbmsBuilder.mapFeatureToRdbms(filter.getFeature(), builderContext)`.
- ancestor joins, added via `rdbmsBuilder.addAncestorJoins` only when
  `builderContext.getAncestors()` already contains the filter.
- one `RdbmsQueryJoin` per aggregated `SubSelect` under the filter, wrapping an
  `RdbmsResultSet` built `withoutFeatures(true)`, `outer(true)`, joined on
  `RdbmsAliasUtil.getOptionalParentIdColumnAlias(filter)`. The `group` flag — and with it
  `partnerTable`/`partnerColumnName` (`StatementExecutor.ID_COLUMN_NAME`) — is set only
  when the sub-select's first navigation join partner is an ancestor container node.

**Rendering (`toSql`)**

Derives a nested prefix with `RdbmsAliasUtil.getFilterPrefix(prefix)` and registers it in
a **copy** of `converterContext.getPrefixes()` keyed by the filter, so nested filters do
not clobber the caller's prefix map. The correlation alias comes from `filter.eContainer()`:
a `SubSelect` container with navigation joins uses `AGGREGATE_PREFIX + alias`, without
navigation joins it uses the sub-select's `getSelect().getAlias()`, any other container
uses its own alias. Joins are emitted in `RdbmsJoinComparator` order after each SQL string
is computed in map order.

**Contracts a caller can violate**

- `checkArgument(from != null || joins.size() < 2, "Size of JOINs must be at most 1 if
  FROM is not set")` — a typeless filter carrying two or more joins fails fast at render
  time.
- Both constructor parameters are `@NonNull`; `filter.eContainer()` is cast to `Node`
  unconditionally, so a filter detached from the query tree throws.
- The subquery correlates on `StatementExecutor.ID_COLUMN_NAME` on both sides — a filter
  whose partner table has no ID column cannot be expressed here.
