# `RdbmsResultSet.java`

The statement assembler of the RDBMS query layer (768 lines, read at signature level per
contract R5). One instance corresponds to one `SubSelect` node and renders a complete
`SELECT … FROM … JOIN … WHERE … GROUP BY … ORDER BY … LIMIT … OFFSET`.

**Key exports**

- `@Builder private RdbmsResultSet(@NonNull SubSelect query, RdbmsBuilderContext
  builderContext, boolean filterByInstances, DAO.Seek seek, boolean withoutFeatures,
  Map<String,Object> mask, boolean skipParents, boolean count)` — the builder is the only
  entry point; the constructor performs the whole assembly.
- `toSql(SqlConverterContext)` (override of `RdbmsField`).
- `@Getter` exposure of the accumulated `joins` / `conditions` state used by enclosing
  builders.

**Assembly order in the constructor**

Derives a private `RdbmsBuilderContext` via `builderContext.toBuilder()` carrying this
result set's own `ancestors`/`descendants` maps, resolves `baseTableName` from
`query.getSelect().getType()`, then in sequence: `addColumnFeatures` (mask-filtered),
`addAncestorJoins`, `addAggregatedFeatures`, `addFilterByInstancesConditions` (only when
`filterByInstances`), `addSubSelectJoins` (skipped when `withoutFeatures` unless the
select is aggregated), `isGrouped`, then either `addNavigationJoins` (when navigation
joins exist) or `addOrderByFeatures`, per-`OrderBy` field collection, `addFilterJoins`,
seek conditions, and a final ancestor-join pass.

**Rendering helpers**

`getSelect` emits `SELECT COUNT (1)` when `count` is set and otherwise the column SQL
**sorted alphabetically** (explicitly "for debugging purposes only" — do not depend on
column order); `DISTINCT` is added only when a limit is set, multiple paths exist and
`skipParents` holds. `getBaseTableName` falls back to the dialect's dual table
(`rdbmsBuilder.getDialect().getDualTable()`) when there is no base table and no joins.
`getJoin` runs `fixStaticJoins()` first, sorts with `RdbmsJoinComparator`, and harvests
`joinConditionTableAliases` while streaming. `fixStaticJoins` re-parents joins whose
partner table is not referenced by any other join onto `query.getSelect()`, clearing
`partnerColumnName` and forcing `outer(true)`, then drops the orphaned static join.

**Seek paging**

`addSeekBaseTableOrderByConditions` and `addSeekOrderByConditions` translate
`DAO.Seek.getLastItem()` into a lexicographic `(orderBy…, id)` comparison, coercing the
last item's values through `rdbmsBuilder.getCoercer()` and matching transfer attributes to
entity attributes by `getSourceAttribute()`. A trailing `RdbmsOrderBy` on
`StatementExecutor.ID_COLUMN_NAME` is always appended when `seek != null`, descending iff
`seek.isReverse()`, to make the ordering total.

**Contracts a caller can violate**

- `query` is `@NonNull`; `builderContext.getRdbmsBuilder()` is cast to `RdbmsBuilder`, so
  a foreign builder implementation throws `ClassCastException`.
- Seek paths are entered only when `query.getLimit() != null && seek != null &&
  seek.getLastItem() != null` — passing a seek without a limit silently produces an
  unpaged statement.
- The `ancestors`/`descendants` maps are this instance's own and are handed to nested
  builders through the derived context; mutating the caller's maps afterwards does not
  affect already-rendered SQL.
- `count` suppresses the select list entirely, so a counting result set must not be reused
  to read column values.
