# `QueryFactory.java`

Runtime entry point of the logical query model. Answers `getQuery(EClass)` → `Optional<Select>`, `getNavigation(EReference)` / `getDataQuery(EAttribute)` → `Optional<SubSelect>`,
`isOrdered(EReference)`, `isStaticReference(EReference)`, `isStaticAttribute(EAttribute)`, `getEntityTypeExpressionsMap()` (read-only `Map<EClass, hu.blackbelt.judo.tatami.asm2query.EntityTypeExpressions>`),
`getNextSourceIndex()` / `getNextTargetIndex()` (`AtomicInteger`s shared with request-time conversions) and `dataExpressionToFeature(DataExpression, hu.blackbelt.judo.tatami.asm2query.Context, Target, EAttribute)`.
Also `getModelAdapter()`, `getQueryModel()` (`QueryModel`), `getQueryModelResourceSupport()`, `isRestored()`, `getCustomJoinDefinitions()`.

**Two construction paths.**
- *Build* (public constructors, `@Builder`): `Asm2Query.executeAsm2Query` over the caller-supplied ASM, measure and expression `ResourceSet`s. Builder arguments: `asmResourceSet`, `measureResourceSet`, `expressionResourceSet`, `coercer` (`@Deprecated`, ignored, may be `null`), `customJoinDefinitions` (null → mutable `ConcurrentHashMap`), `modelName` (default: name of the ASM model). Custom joins are read once while building (copied into asm2query's value type); an entry added afterwards has no effect. Validates the model (`IllegalStateException("Invalid query model")`). Leaves the historic side effect of the build on the expression model (iterators of collection expressions).
- *Restore* (`restore(AsmModel, MeasureModel, QueryModel)`, `restore(ResourceSet, ResourceSet, QueryModel)`): wraps `QueryIndex.restore` of a loaded `QueryModel` (resolved against the live ASM/expression resources, see `QueryModelPersistence.load`); validates it; creates `DataExpressionConverter` (`AsmModelAdapter`, `JoinFactory`, `FeatureFactory`) for dynamic filters; no collector and no expression evaluator. `getCustomJoinDefinitions()` is a mutable empty map; custom joins are never applied (the decision selects the build path when any is configured).

**Contracts.** `getEntityTypeExpressionsMap()` is unmodifiable (callers: `AttributeSelectorTranslator` reads it); the former `Context` / `EntityTypeExpressions` types moved to `hu.blackbelt.judo.tatami.asm2query` (BREAKING for callers of `dataExpressionToFeature` and `getEntityTypeExpressionsMap`). Use `QueryModelLoadOrBuild` to choose the path.

Tests: `QueryFactoryRestoreTest` (built vs restored equality, live references, dynamic expressions), `QueryFactoryBuildTest` (coercer ignored, custom joins at build time only). See change: persisted-query-model
