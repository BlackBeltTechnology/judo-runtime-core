## ADDED Requirements

### Requirement: Query factory build path delegates to Asm2Query
The `QueryFactory` build path SHALL delegate to `Asm2Query` of `judo-tatami-asm2query` over the caller-supplied ASM, measure and expression `ResourceSet`s, copying the custom joins into the builder's own value type. The `coercer` constructor argument SHALL stay, be `@Deprecated` and be ignored.

#### Scenario: Build with caller-supplied expression resource set
- **GIVEN** a `QueryFactory` constructed with ASM, measure and expression resource sets, a `coercer` and a custom join map
- **WHEN** the factory is built
- **THEN** `Asm2Query` produces the query model from exactly those resource sets and every public lookup answers as before the move

#### Scenario: Coercer argument is ignored
- **GIVEN** a `QueryFactory` constructed with a non-null `coercer`
- **WHEN** queries are built
- **THEN** the `coercer` is never called and the factory behaves as if it were absent

### Requirement: Query factory restore path
The `QueryFactory` SHALL offer a restore construction path that wraps a `QueryIndex` rebuilt from a loaded `QueryModel`, resolved against the runtime's `AsmModel`, `MeasureModel` and `ExpressionModel`. It SHALL create no `TransferObjectTypeBindingsCollector` and no `ExpressionEvaluator`, and SHALL still support dynamic filters.

#### Scenario: Restored factory equals the built one
- **GIVEN** a query model built, saved and loaded for the same ASM, measure and expression models
- **WHEN** every public method is called on a restored and on a built `QueryFactory`
- **THEN** every answer is equal: `getQuery`, `getNavigation`, `getDataQuery`, `isOrdered`, `isStaticReference`, `isStaticAttribute`, `getEntityTypeExpressionsMap`, `getNextSourceIndex` and `getNextTargetIndex`

#### Scenario: Restored references are the live runtime objects
- **GIVEN** a restored `QueryFactory`
- **WHEN** its non-containment references into the ASM are inspected
- **THEN** each target is the same object as in the runtime's ASM resource

#### Scenario: Restored factory has mutable empty custom joins
- **GIVEN** a restored `QueryFactory`
- **WHEN** `getCustomJoinDefinitions()` is called
- **THEN** a mutable, empty map is returned, as for a built factory

#### Scenario: Dynamic filter aliases do not collide
- **GIVEN** a restored `QueryFactory` and a request with a dynamic filter
- **WHEN** `SelectStatementExecutor` creates the filter
- **THEN** new source and target indexes continue after the restored `nextSourceIndex` and `nextTargetIndex`

### Requirement: QueryModelLoadOrBuild decides between restore and build
`QueryModelLoadOrBuild` SHALL be the only place that chooses between restoring a persisted query model and building one. It SHALL take the live ASM, measure and expression models with their file digests, an optional `PersistedQueryModel` locator, the custom joins and the mode.

#### Scenario: Valid artifact is restored
- **GIVEN** mode `auto`, a locator whose artifact has matching format version, generator, model name and digests, and no custom joins
- **WHEN** the decision is taken
- **THEN** the query model is restored and an INFO line reports the restore with its duration

#### Scenario: Artifact absent
- **GIVEN** mode `auto` and no locator or no query file
- **WHEN** the decision is taken
- **THEN** the container's existing build runs and no WARN is logged

#### Scenario: Digest or stamp mismatch
- **GIVEN** mode `auto` and an artifact whose `asmDigest`, `measureDigest`, `expressionDigest`, `formatVersion`, `generator` or `modelName` differs from the runtime's
- **WHEN** the decision is taken
- **THEN** a WARN names the mismatching stamp with both values and the build runs

#### Scenario: Failure or unresolved proxies
- **GIVEN** mode `auto` and an artifact that throws an `Exception` or `Error` while being read, parsed, resolved, validated or restored, or leaves unresolved proxies
- **WHEN** the decision is taken
- **THEN** a WARN with the cause or the proxy count is logged and the build runs

#### Scenario: Custom joins configured
- **GIVEN** mode `auto` and a non-empty custom join map
- **WHEN** the decision is taken
- **THEN** an INFO "saved query model not used with custom joins" is logged and the build runs

### Requirement: Query model mode
`QueryModelLoadOrBuild` SHALL accept the modes `auto` (default), `load` and `build`. `build` SHALL run the container's existing build without ever opening the artifact. `load` SHALL fail with an error naming the reason in every case where `auto` falls back, including custom joins.

#### Scenario: Build mode never opens the artifact
- **GIVEN** mode `build` and a corrupt artifact
- **WHEN** the decision is taken
- **THEN** the build runs and the locator's stream supplier is invoked zero times

#### Scenario: Strict load fails on a generator mismatch
- **GIVEN** mode `load` and an artifact stamped with a different generator
- **WHEN** the decision is taken
- **THEN** an error naming both generator identities is raised

#### Scenario: Strict load fails without the artifact
- **GIVEN** mode `load` and no query file
- **WHEN** the decision is taken
- **THEN** an error saying the artifact is missing is raised

#### Scenario: Strict load fails with custom joins
- **GIVEN** mode `load` and a non-empty custom join map
- **WHEN** the decision is taken
- **THEN** an error saying the saved query model cannot be used with custom joins is raised

### Requirement: Builder types moved to judo-tatami-asm2query
The module `judo-runtime-core-query` SHALL contain only `QueryFactory`, `CustomJoinDefinition` and `QueryModelLoadOrBuild`. `FeatureFactory`, `JoinFactory`, `Context`, `Constants`, the feature converters and `EntityTypeExpressions` SHALL live in `hu.blackbelt.judo.tatami.asm2query`. **BREAKING**: `dataExpressionToFeature(..., Context, ...)` and `getEntityTypeExpressionsMap()` change signature.

#### Scenario: Internal callers use the new package
- **GIVEN** `SelectStatementExecutor`, `FunctionMapper` and `AttributeSelectorTranslator`
- **WHEN** runtime-core is compiled
- **THEN** they import `Context`, `Constants` and `EntityTypeExpressions` from `hu.blackbelt.judo.tatami.asm2query`

## MODIFIED Requirements

### Requirement: Query model validation
The `QueryFactory` SHALL validate the generated or restored query model and reject invalid models. The query resource SHALL pass `isValid()` with the `QueryModelInfo` root present.

#### Scenario: Valid query model
- **GIVEN** a correctly configured ASM model with valid expression bindings
- **WHEN** the `QueryFactory` is constructed
- **THEN** `queryModelResourceSupport.isValid()` returns `true` and no exception is thrown

#### Scenario: Invalid query model
- **GIVEN** an ASM model that produces inconsistent query structures
- **WHEN** the `QueryFactory` is constructed
- **THEN** an `IllegalStateException` is thrown with message "Invalid query model"

#### Scenario: Restored query model is validated
- **GIVEN** a loaded query model containing the `QueryModelInfo` root
- **WHEN** the restore path validates it
- **THEN** a valid model restores, and an invalid one makes the decision fall back or, in mode `load`, fail

### Requirement: Custom join support
The `QueryFactory` SHALL support custom SQL-based join definitions for specific references via `CustomJoinDefinition`. Joins SHALL be consumed only while the query model is built; a restored factory carries none, so custom joins always select the build path.

#### Scenario: Reference with custom join definition
- **GIVEN** a `QueryFactory` constructed with a `customJoinDefinitions` map containing a mapping for a derived reference
- **WHEN** the query is built for a transfer object type containing that reference
- **THEN** a `CustomJoin` is created with the configured `navigationSql`, `sourceIdParameterName`, and `sourceIdSetParameterName` instead of the default navigation join chain

#### Scenario: Mutation after construction is a no-op
- **GIVEN** a constructed `QueryFactory`
- **WHEN** an entry is added to the map returned by `getCustomJoinDefinitions()`
- **THEN** the already built queries do not change

## REMOVED Requirements

### Requirement: Expression-to-feature conversion
**Reason**: `FeatureFactory` and its converters move to `judo-tatami-base/judo-tatami-asm2query` (`hu.blackbelt.judo.tatami.asm2query`), the builder's new home.
**Migration**: Use `hu.blackbelt.judo.tatami.asm2query.FeatureFactory`; depend on `judo-tatami-asm2query`.

### Requirement: Navigation-to-join conversion
**Reason**: `JoinFactory` moves to `judo-tatami-base/judo-tatami-asm2query` (`hu.blackbelt.judo.tatami.asm2query`).
**Migration**: Use `hu.blackbelt.judo.tatami.asm2query.JoinFactory`; depend on `judo-tatami-asm2query`.

### Requirement: Query context management
**Reason**: `Context` moves to `judo-tatami-base/judo-tatami-asm2query` (`hu.blackbelt.judo.tatami.asm2query`).
**Migration**: Import `Context` from `hu.blackbelt.judo.tatami.asm2query`; callers of `dataExpressionToFeature` recompile.
