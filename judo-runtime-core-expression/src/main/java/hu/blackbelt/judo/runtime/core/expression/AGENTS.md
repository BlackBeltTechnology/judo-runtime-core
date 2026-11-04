# AGENTS.md — `judo-runtime-core-expression/src/main/java/hu/blackbelt/judo/runtime/core/expression`

Binding side of the expression model: resolves `AttributeBinding` / `ReferenceBinding` /
`FilterBinding` elements of the expression resource set against ASM transfer object types
and keeps the resulting getter/setter expression maps for the query builders.

| File | Purpose |
| --- | --- |
| `EntityTypeExpressions.java` | Entity-side half of the binding maps: `@Builder` over `@NonNull entityType`, with `getterAttributeExpressions` (`EAttribute` → `DataExpression`) and `getterReferenceExpressions` (`EReference` → `ReferenceExpression`). Both maps are `final ConcurrentHashMap` outside the builder, so callers mutate them in place; no setter expressions exist on the entity side. |
| `MappedTransferObjectTypeBindings.java` | Node of the mapped transfer object expression tree. `@Builder` pins `@NonNull entityType` + `@NonNull transferObjectType`; holds getter/setter maps for attributes and references, child `references` (`EReference` → `MappedTransferObjectTypeBindings`), and a `@Setter filter` `LogicalExpression`. `toString` prints `FROM`/`TO` FQ names plus each binding. |
| `TransferObjectTypeBindingsCollector.java` | → see `TransferObjectTypeBindingsCollector.java.AGENTS.md` — walks ASM transfer object types and collects their expression bindings. |
| `UnmappedTransferObjectTypeBindings.java` | Binding holder for transfer object types with no mapped entity. `@Builder` over `@NonNull unmappedTransferObjectType`, plus `dataExpressions` and `navigationExpressions` maps populated only from GETTER-role bindings of derived features. `toString` renders `FROM: <fqName>` with one line per data/navigation expression. |
