# AGENTS.md — `judo-runtime-core-dao-core/src/main/java/hu/blackbelt/judo/runtime/core/dao/core/processors`

Payload-to-statement translation layer. Each processor takes a transfer-object `EClass` plus a
`Payload` (or identifiers), walks the mapped entity graph via `InstanceCollector`, and returns an
immutable `Collection<Statement>` for the storage-side executor. Processors never touch storage
themselves — they only plan.

| File | Purpose |
| --- | --- |
| `AddReferencePayloadDaoProcessor.java` | Builds `AddReferenceStatement` per identifier for one `EReference`. For bidirectional references it pre-collects `alreadyReferencingInstances` from `getBackReferences()` (excluding `parentIdentifier`) so the executor can detach prior holders. `existenceCheck` adds `InstanceExistsValidationStatement`. `reference` and `identifiers` are `checkArgument`-mandatory; result is `ImmutableSet`. |
| `DeletePayloadDaoProcessor.java` | Recursive delete planner: expands the `InstanceGraph` into `DeleteStatement`/`RemoveReferenceStatement` and enforces mandatory back-references. → see `DeletePayloadDaoProcessor.java.AGENTS.md` |
| `InsertPayloadDaoProcessor.java` | Recursive insert planner over a mapped transfer-object payload; applies default values, embeds compositions, links associations. → see `InsertPayloadDaoProcessor.java.AGENTS.md` |
| `PayloadDaoProcessor.java` | Shared base: collaborators, payload/feature predicates, payload key constants, and the mandatory/structure checks all processors reuse. → see `PayloadDaoProcessor.java.AGENTS.md` |
| `RemoveReferencePayloadDaoProcessor.java` | Builds `RemoveReferenceStatement` per identifier for one `EReference`, optionally preceded by `InstanceExistsValidationStatement`. `reference` and `parentIdentifier` are `checkArgument`-mandatory; `identifiers` is dereferenced unguarded, so null throws NPE. The `reference.isRequired()` branch is an empty placeholder — removing a mandatory reference is **not** rejected here. |
| `UpdatePayloadDaoProcessor.java` | Diffs original vs. updated payload into insert/update/delete/reference statements, owning the embedded-vs-associated rules and optimistic locking. → see `UpdatePayloadDaoProcessor.java.AGENTS.md` |
