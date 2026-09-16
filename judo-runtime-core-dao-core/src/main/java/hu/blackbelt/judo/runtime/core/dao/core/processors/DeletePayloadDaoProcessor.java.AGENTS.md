# DeletePayloadDaoProcessor.java

Recursive delete planner: expands the collected `InstanceGraph` into `DeleteStatement`, `RemoveReferenceStatement` and `InstanceExistsValidationStatement` set.
`delete(EClass, Collection<Serializable>)` validates inputs with `checkArgument`, resolves mapped entity via `getAsmUtils().getMappedEntityType` (fails `checkState` when un-mapped), emits existence checks for every id, then walks `getInstanceCollector().collectGraph(entityType, ids)`.
`statementCollector` emits `DeleteStatement` (cyclic guard: skips when instance already has one), removes references/back-references, recurses containments with container + `EReference` context and recurses `reverseCascadeDelete`-annotated refs (`AsmUtils.annotatedAsTrue`).
`checkMandatoryBackReferences` throws `checkState` ("There are mandatory references that cannot be removed") when a `lowerBound > 0` back reference survives the delete.
Caller passes mapped transfer object + ids; violations surface as `IllegalArgumentException` / `IllegalStateException`.