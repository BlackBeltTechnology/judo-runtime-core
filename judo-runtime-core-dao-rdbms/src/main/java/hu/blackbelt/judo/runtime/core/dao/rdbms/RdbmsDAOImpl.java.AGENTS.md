# `RdbmsDAOImpl.java`

Concrete `AbstractRdbmsDAO` (933 lines, read at signature level per contract R5).
Its own javadoc states it "contains all plumbing logic for `AbstractRdbmsDAO`" and
is never auto-created — lifecycle is owned by the DataSource/model-dependent wiring
or by direct constructor use.

## Collaborators (all `@NonNull`, Lombok `@Builder` constructor)

`asmModel`, `dataSource`, `identifierProvider`, `context`, `metricsCollector`,
`instanceCollector`, `modifyStatementExecutor`, `selectStatementExecutor`,
`queryFactory`, plus optional `Boolean optimisticLockEnabled` which defaults to
`true` via `requireNonNullElse`.

## Implemented abstract hooks

Read side: `readStaticFeatures`, `readStaticData`, `readAll`, `countAll`,
`searchByFilter`, `countByFilter`, `readAllReferences`, `countAllReferences`,
`readByIdentifier(s)`, `searchReferences`, `countReferences`,
`readMetadataByIdentifier`, `readDefaultsOf`, `applyDeepDefaultsOf`, `readRangeOf`,
`calculateNumberRangeOf` — all routed through `queryFactory` +
`selectStatementExecutor`.

Write side: `insertPayload`, `insertPayloadAndAttach`, `updatePayload`,
`deletePayload`, `setReferenceOfInstance`, `unsetReferenceOfInstance`,
`addReferencesOfInstance`, `removeReferencesOfInstance` — each builds DAO statements
through the payload processors (`getInsertPayloadProcessor`,
`getUpdatePayloadProcessor`, `getDeletePayloadProcessor`,
`getAddReferencePayloadProcessor`, `getRemoveReferencePayloadProcessor`) and hands
them to `modifyStatementExecutor`. All declare `throws SQLException`.

## Contracts a caller can violate

- **Stateless guard.** Every mutating entry point `checkState`s that the `Context`
  key `STATEFUL` is not `FALSE` unless `ROLLBACK` is `TRUE`, failing with
  `"<OP> is not supported in stateless operation"` (INSERT, UPDATE, DELETE, SET,
  UNSET, ADD, REMOVE). Mutations from a stateless dispatch are rejected, not
  silently dropped.
- **Cardinality / containment.** Attaching over a full single-valued containment
  throws `IllegalArgumentException("Upper cardinality violated")` or
  `"Containment already set"`.
- **Range queries** require an input payload (`"Missing input to get range"`) and a
  persisted instance (`"The given instance (payload) is not stored"`).
- **Optimistic locking** is on unless explicitly constructed with `false`; the flag
  is forwarded into the update statement build.

## Payload marker keys

`CREATED` = `__$created` and `DEFAULT_VALUES_LOADED_KEY` = `__defaultValuesLoaded`
are public payload markers; `STATEFUL` / `ROLLBACK` are private `Context` keys.
Per-`EClass` defaults presence is memoised in `hasDefaultsMap`.
