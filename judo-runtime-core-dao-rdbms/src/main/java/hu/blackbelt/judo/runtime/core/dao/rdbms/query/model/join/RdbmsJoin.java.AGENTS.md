# `RdbmsJoin.java`

Abstract base of every JOIN fragment in the RDBMS query layer. It owns the JOIN keyword,
the default ON condition (including many-to-many junction tables), and the alias
bookkeeping the join ordering depends on.

**Key exports**

- `@SuperBuilder @NoArgsConstructor @Slf4j public abstract class RdbmsJoin` with builder
  state `columnName`, `partnerTable` (`@Getter @Setter Node`), `partnerTablePrefix`,
  `partnerTablePostfix`, `partnerColumnName` (`@Getter @Setter`), `junctionTableName`,
  `junctionColumnName`, `junctionOppositeColumnName`, `@Getter alias`,
  `@Getter @Setter boolean outer`, and `@NonNull @Singular Collection<RdbmsField>`
  `conditions` / `onConditions`.
- `@Getter aliasToCompareWith` and `@Getter Set<String> joinConditionTableAliases` — the
  inputs `RdbmsJoinComparator` reads.
- `public String toSql(SqlConverterContext, boolean fromIsEmpty)`.
- `public Collection<String> conditionToSql(SqlConverterContext)` — renders `conditions`
  with `includeAlias(false)`; these land in the enclosing `WHERE`, not in the `ON`.
- `protected getPartnerTableName(String prefix, Map<Node,String> prefixes)`.
- `protected getJoinCondition(SqlConverterContext)` — overridable.
- `protected abstract String getTableNameOrSubQuery(SqlConverterContext)`.

**`toSql`**

Computes the join condition first, then `"<table> AS <prefix><alias>"`. With
`fromIsEmpty` it emits `\nFROM <table>` and **drops the ON condition entirely**;
otherwise `\nLEFT OUTER JOIN` when `outer`, else `\nJOIN`, followed by
`ON (<condition>)`. It registers `<prefix><alias>` in `joinConditionTableAliases` and
sets `aliasToCompareWith`, so the comparator only sees correct dependencies **after**
`toSql` has run for every join.

**`getJoinCondition` — three shapes**

1. `junctionTableName != null` → `EXISTS (SELECT 1 FROM <junction> WHERE
   <partner>.<partnerColumnName> = <junction>.<junctionOppositeColumnName> AND
   <junction>.<junctionColumnName> = <prefix><alias>.<columnName>)`.
2. partner name, `partnerColumnName` and `columnName` all present → plain
   `<partner>.<partnerColumnName> = <prefix><alias>.<columnName>`.
3. otherwise → the degenerate `1 = 1`, i.e. a cross join. This is silent: a join built
   with a partner table but no `partnerColumnName` produces a cartesian product rather
   than an error.

`onConditions` are appended with ` AND ` to whichever shape was chosen.

**Partner prefixing**

`getPartnerTableName` prefers an explicit per-node prefix from
`converterContext.getPrefixes()` over the ambient prefix, then wraps the partner's alias
with `partnerTablePrefix`/`partnerTablePostfix`. The resolved name is added to
`joinConditionTableAliases`; a null `partnerTable` yields `null` and records nothing.

**Contracts a caller can violate**

- `conditions` vs `onConditions` are not interchangeable — `conditions` never reach the
  `ON` clause, so putting an outer-join-sensitive predicate there changes result
  semantics.
- Junction mode requires all three junction fields plus a non-null partner name; a null
  `partnerTableName` there produces `List.of(null, …)` and throws `NullPointerException`.
- `joinConditionTableAliases` is a mutable shared `Set` exposed by `@Getter`; enclosing
  result sets drain it to inherit dependencies.
