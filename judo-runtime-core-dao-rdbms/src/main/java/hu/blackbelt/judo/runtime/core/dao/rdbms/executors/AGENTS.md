# `...dao.rdbms.executors` — agent notes

Turns `hu.blackbelt.judo.runtime.core.dao.core.statements.*` statement objects into
JDBC work over a Spring `NamedParameterJdbcTemplate`. `ModifyStatementExecutor` is
the only public write entry point (plus `SelectStatementExecutor` for reads); every
per-statement executor is package-private and instantiated by it. All share the
`StatementExecutor` base and log on topic `dao-rdbms`.

| File | Purpose |
|---|---|
| `AddReferenceStatementExecutor.java` | Executes `AddReferenceStatement`s. `executeAddReferenceStatements` decides from the RDBMS `Rule` whether the foreign key sits in the owner table (issue UPDATE) or in a junction table (INSERT a join row). Fails via `checkState` when an update touches no row ("no records updated on delete reference") or when a junction row already exists (expected count 0). |
| `AddRemoveReferenceStatementConsistencyCheckExecutor.java` | Pre-flight crosscheck via `checkRemoveReferenceStatements`/`checkAddReferenceStatements`, no SQL mutation → see `AddRemoveReferenceStatementConsistencyCheckExecutor.java.AGENTS.md` |
| `CheckUniqueAttributeStatementExecutor.java` | Executes `CheckUniqueAttributeStatement`s by querying existing rows per unique `EAttribute` / `AttributeValue`. Both the batch pre-check and the per-statement path throw `IllegalStateException("Identifier uniqueness violation(s): ...")` listing the offending attribute/value pairs. Only entity types pass (`AsmUtils.isEntityType`). |
| `DeleteStatementExecutor.java` | Executes `DeleteStatement`s, consuming embedded `RemoveReferenceStatement`s in the same pass. Orders deletes by jgrapht `TopologicalOrderIterator` over a `DefaultDirectedGraph` of foreign-key dependencies so children go first. Each DELETE `checkState`s that it removed 0 or 1 rows ("Maximum of 1 record should have been deleted"). |
| `EntityExistsValidationStatementExecutor.java` | Executes `InstanceExistsValidationStatement`s: per type+id, SELECTs the row and throws `ValidationException("Instance not found", ...)` carrying a `ValidationResult` at `Level.ERROR` when absent, plus a `checkState` that exactly one row matched. Runs first in the modify pipeline so later statements never target a missing instance. |
| `InsertStatementExecutor.java` | Executes `InsertStatement`s and the `AddReferenceStatement`s embedded in them. Rejects non-entity and abstract targets via `checkState` ("Non-entity types cannot be explicitly instantiated", "Abstract types cannot be explicitly instantiated"). Populates `VERSION`, `CREATE_TIMESTAMP`, `CREATE_USER_ID`, `CREATE_USERNAME` meta columns and topologically orders inserts over foreign-key edges. |
| `ModifyStatementExecutor.java` | Public façade: `executeStatements(NamedParameterJdbcTemplate, Collection<Statement>)` → see `ModifyStatementExecutor.java.AGENTS.md` |
| `RemoveReferenceStatementExecutor.java` | Executes `RemoveReferenceStatement`s: nulls the foreign key in the owner table, or deletes the junction row when the reference is mapped to a join table. Every affected UPDATE/DELETE `checkState`s exactly one row changed, failing with "no records updated on delete reference". |
| `SelectStatementExecutor.java` | Public read path: `executeSelect` overloads, `countSelect`, `selectMetadata`. → see `SelectStatementExecutor.java.AGENTS.md` |
| `SelectStatementExecutorQueryMetaCache.java` | Precomputes per `SubSelect` + mask + reference chain the alias→`Node`/alias→`Target` maps the select result mapper needs → see `SelectStatementExecutorQueryMetaCache.java.AGENTS.md` |
| `StatementExecutor.java` | Abstract base creating `RdbmsReferenceUtil`, declaring `ID_COLUMN_NAME`/`ENTITY_TYPE_COLUMN_NAME` key constants → see `StatementExecutor.java.AGENTS.md` |
| `UpdateReferenceExecutor.java` | Coalesces all `AddReferenceStatement`/`RemoveReferenceStatement`s sharing one entity identifier into a single UPDATE, so a batch that both clears and sets FKs on the same row issues one statement. `checkState`s exactly one row updated. |
| `UpdateStatementExecutor.java` | Executes `UpdateStatement`s in two SQL steps with optimistic locking on `VERSION` (`AND VERSION = :__version`) → see `UpdateStatementExecutor.java.AGENTS.md` |
