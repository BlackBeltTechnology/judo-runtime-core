## ADDED Requirements

### Requirement: Query Factory Through the Load-or-Build Decision
`QueryFactoryProvider` SHALL obtain the `QueryFactory` through `QueryModelLoadOrBuild`, passing the loader's models and digests, its `PersistedQueryModel` locator, the configured custom joins and the `queryModelMode`. Its build fallback SHALL be the provider's existing build. A `QueryFactory` instance supplied in `JudoDefaultModuleConfiguration` SHALL still bypass the decision.

#### Scenario: Valid artifact restored at injector creation
- **GIVEN** a `JudoModelLoader` with a valid `<name>-query.model`, no custom joins and mode `auto`
- **WHEN** the `QueryFactory` is first injected
- **THEN** it is restored, an INFO "query model restored" line with its duration is logged, and the JQL extractor is not invoked for the query model

#### Scenario: Artifact absent or stale
- **GIVEN** a `JudoModelLoader` without a query file, or with one that is stale or corrupt, and mode `auto`
- **WHEN** the `QueryFactory` is first injected
- **THEN** the existing build runs and the injector starts normally

#### Scenario: Supplied QueryFactory bypasses the decision
- **GIVEN** a `JudoDefaultModuleConfiguration` with a non-null `queryFactory`
- **WHEN** `QueryFactory` is injected
- **THEN** the supplied instance is bound and `QueryModelLoadOrBuild` is not called

### Requirement: Query Model Mode Option
`JudoDefaultModuleConfiguration` SHALL expose a `queryModelMode` option with the values `auto` (default), `load` and `build`, and the `JudoDefaultModule` builder SHALL copy it, with a `configuration` object taking precedence.

#### Scenario: Default mode
- **GIVEN** a `JudoDefaultModule` built without `queryModelMode`
- **WHEN** the `QueryFactory` is created
- **THEN** mode `auto` is used

#### Scenario: Builder copies the mode
- **GIVEN** a `JudoDefaultModule.builder()` call with `queryModelMode` set to `load` and no `configuration` object
- **WHEN** the module is built
- **THEN** the resulting configuration carries mode `load`

## MODIFIED Requirements

### Requirement: Model Binding
The module SHALL bind all JUDO metamodels (AsmModel, RdbmsModel, MeasureModel, ExpressionModel, LiquibaseModel) as instances loaded from `JudoModelLoader`, and conditionally bind `KeycloakModel` when present. It SHALL bind no second `ExpressionModel` and no query-side model: the persisted query model is reached only through the loader's `PersistedQueryModel` locator.

#### Scenario: All metamodels are bound from JudoModelLoader
- **GIVEN** a `JudoDefaultModule` configured with a valid `JudoModelLoader` containing all required models
- **WHEN** the Guice injector is created
- **THEN** `AsmModel`, `RdbmsModel`, `MeasureModel`, `ExpressionModel`, and `LiquibaseModel` SHALL be injectable as singleton instances

#### Scenario: KeycloakModel is conditionally bound
- **GIVEN** a `JudoModelLoader` whose `getKeycloakModel()` returns a non-null `KeycloakModel`
- **WHEN** `configureModels()` is invoked
- **THEN** `KeycloakModel` SHALL be bound as an instance

#### Scenario: KeycloakModel is not bound when absent
- **GIVEN** a `JudoModelLoader` whose `getKeycloakModel()` returns null
- **WHEN** `configureModels()` is invoked
- **THEN** no binding for `KeycloakModel` SHALL be created

#### Scenario: No second ExpressionModel
- **GIVEN** an injector with a valid query artifact present
- **WHEN** `getInstance(ExpressionModel.class)` is called
- **THEN** it returns the loader's `-expression.model` instance and no binding for a query-side expression model exists

### Requirement: Model Loading from Multiple Sources
`JudoModelLoader` SHALL support loading JUDO models from classpath, filesystem directory, and URL, selecting the correct dialect-specific RDBMS model file. While loading it SHALL compute the SHA-256 digest of the complete bytes of the ASM, measure and expression model files, and SHALL build a `PersistedQueryModel` locator for `<name>-query.model` without reading that file.

#### Scenario: Load models from classpath
- **GIVEN** model files present in the classpath under a `model/` directory matching the pattern `{modelName}-asm.model`
- **WHEN** `JudoModelLoader.loadFromClassloader(modelName, classLoader, dialect, validate, loadKeycloak)` is called
- **THEN** all required models (ASM, RDBMS, Measure, Expression, Liquibase, transformation trace) SHALL be loaded and returned

#### Scenario: Load models from filesystem directory
- **GIVEN** a valid directory containing model files
- **WHEN** `JudoModelLoader.loadFromDirectory(modelName, directory, dialect, loadKeycloak)` is called
- **THEN** models SHALL be loaded using the directory URI as the base

#### Scenario: Empty model loader for testing
- **GIVEN** a request for an empty model set
- **WHEN** `JudoModelLoader.empty()` is called
- **THEN** empty but valid ASM, RDBMS, Measure, Expression, Liquibase, and Keycloak models SHALL be created with default structures

#### Scenario: Digests and locator from directory, classpath and URL
- **GIVEN** model files and a query artifact in a directory, inside a classpath jar, or at a file URL
- **WHEN** the loader loads the models
- **THEN** the ASM, measure and expression digests are recorded, the locator is present, and the query file has not been opened

#### Scenario: No query artifact
- **GIVEN** a model location without `<name>-query.model`
- **WHEN** the loader loads the models
- **THEN** the loader carries no locator and loading succeeds
