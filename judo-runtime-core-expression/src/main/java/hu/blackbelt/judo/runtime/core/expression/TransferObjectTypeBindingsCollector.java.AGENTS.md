# `TransferObjectTypeBindingsCollector.java`

Loops over ASM transfer object types and builds the expression binding trees the query
and DAO layers consume. Reads bindings out of the expression `ResourceSet`, matching them
by `namespace::name` reconstructed from the ASM classifier FQ name with `.` replaced by `::`.

**Key exports**

- `TransferObjectTypeBindingsCollector(ResourceSet asmResourceSet, ResourceSet expressionResourceSet)`
  — builds `AsmUtils` over the ASM set, inits an `ExpressionEvaluator` from the `Expression`
  contents of the FIRST expression resource only, and pre-seeds `entityTypeExpressionsMap`
  with an empty `EntityTypeExpressions` for every `AsmUtils.isEntityType` class.
- `Map<EClass, EntityTypeExpressions> getEntityTypeExpressionsMap()` — unmodifiable view; the
  per-entity maps inside it are still mutated as graphs get collected.
- `Optional<MappedTransferObjectTypeBindings> getTransferObjectGraph(EClass)` and the
  `(EClass, Map<EClass, MappedTransferObjectTypeBindings>)` overload carrying the processed map.
- `Optional<UnmappedTransferObjectTypeBindings> getTransferObjectBindings(EClass)`.
- `Boolean isStaticAttribute(EAttribute)` / `Boolean isStaticReference(EReference)`.
- `<T> Stream<T> getExpressionElement(Class<T>)` — streams `expressionResourceSet.getAllContents()`
  filtered by assignability.

**Contracts a caller can violate**

- `getTransferObjectGraph` returns `empty()` when `asmUtils.getMappedEntityType` finds no mapped
  entity — an unmapped type must go through `getTransferObjectBindings` instead, and vice versa:
  `getTransferObjectBindings` returns `empty()` for entity types and mapped transfer object types.
- Recursion over `getEAllReferences` is cycle-guarded solely by the `processedMappedTransferObjectTypeBindings`
  map; callers sharing one collection across calls get shared, already-built nodes back. Note the
  map is keyed by the *entity* type on lookup but populated with the *transfer object* type.
- Binding role dispatch is strict: a GETTER/SETTER `AttributeBinding` whose expression is not a
  `DataExpression`, a `ReferenceBinding` whose expression is not a `ReferenceExpression`, and a
  `FilterBinding` whose expression is not a `LogicalExpression` each throw `IllegalStateException`.
- A transfer feature carrying the `binding` extension annotation whose named feature is absent from
  the mapped entity type throws `IllegalStateException("Attribute not found")` / `("Reference not found")`.
- Static flags are computed lazily from `expressionEvaluator.getVariablesOfScope` during GETTER
  collection; `isStaticAttribute` / `isStaticReference` return `null` (not `false`) for a feature
  whose graph has not been collected yet, so callers must null-check.
- Bindings are collected only when a `TypeName` exists for the mapped entity type; a missing one is
  logged at WARN and silently yields a bindings node with no attribute or reference expressions.
