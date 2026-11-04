# `RdbmsInstanceCollector.java`

`InstanceCollector` implementation over `NamedParameterJdbcTemplate` (870 lines, read
at signature level per contract R5). Given an entity type and identifiers it returns
the `InstanceGraph` of reachable instances — the input every cascade delete and
reference-fixup needs before statements are built.

## Collaborators (`@NonNull`, Lombok `@Builder` constructor)

`jdbcTemplate`, `asmModel` (kept as `AsmUtils`), `rdbmsResolver`, `rdbmsModel`,
`coercer`, `identifierProvider`, `rdbmsParameterMapper`.

## Public surface

- `collectGraph(EClass, Collection<Serializable>)` → `Map<Serializable, InstanceGraph>`
- `collectGraph(EClass, Serializable)` → single `InstanceGraph`
- `createSelects()` — precomputes one `RdbmsSelect` per entity type
- `getRdbmsSupport()` — lazily builds and caches `RdbmsModelResourceSupport`

## Internal SQL model

Private nested types form the query tree: `Source` / `BaseSource` / `Joinable`
interfaces, `RdbmsSelect` (holds `joins`, `subSelects`, renders via `toSql()` and
`toSql(RdbmsSubSelect)`), `RdbmsJoin`, and `RdbmsSubSelect`. Table and column names
come from `rdbmsResolver`; `rdbmsRules.Rule` decides foreign-key direction, including
the inverse cases annotated in the join-building code.

## Contracts

- `createSelects()` is guarded by `selectsCreated` (`AtomicBoolean`) and caches into
  `selectsByEntityType`; callers must not assume repeated calls rebuild the plan.
- Join aliases are handed out by `nextAliasIndex` (`AtomicInteger`), so alias
  identity is per-collector-instance, not per-query — do not persist an alias.
- `getRdbmsSupport()` caches into an `AtomicReference`; a mutated `RdbmsModel` after
  first call is not picked up.
- Lookups `checkArgument` on the entity type being mapped and report failures with
  `AsmUtils.getClassifierFQName`.
