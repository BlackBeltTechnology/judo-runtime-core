# `QueryFactory.java`

Builds `Select`/`SubSelect` query graphs for mapped transfer object types. Exports `getQuery(EClass)` → `Optional<Select>`,
`getNavigation(EReference)`/`getDataQuery(EAttribute)` → `Optional<SubSelect>`, `isOrdered(EReference)`, `isStaticReference(EReference)`,
`isStaticAttribute(EAttribute)`, `dataExpressionToFeature(DataExpression, Context, Target, EAttribute)`. Returns `Optional.empty()`
for unmapped types; caches navigation/data subqueries in `ConcurrentHashMap`s keyed by reference/attribute. `customJoinDefinitions`
(`Map<EReference, CustomJoinDefinition>`, `@Getter`) backs static references — callers register custom join SQL there. Alias countdown
fields `nextSourceIndex`/`nextTargetIndex` are `@Getter`. `@Builder` constructor takes `ResourceSet` asm + expression resources and `Coercer`.