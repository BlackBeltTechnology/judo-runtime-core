## ADDED Requirements

### Requirement: Query Model Mode Property
The `QueryFactory` bean SHALL read the query model mode from the property `judo.queryModel.mode` with the values `auto` (default), `load` and `build`, and pass it to `QueryModelLoadOrBuild`.

#### Scenario: Default mode
- **GIVEN** a Spring context without `judo.queryModel.mode`
- **WHEN** the `QueryFactory` bean is created
- **THEN** mode `auto` is used

#### Scenario: Strict load without artifact
- **GIVEN** `judo.queryModel.mode=load` and no query artifact on the classpath
- **WHEN** the context starts
- **THEN** startup fails with an error saying the artifact is missing

#### Scenario: Forced build
- **GIVEN** `judo.queryModel.mode=build` and a corrupt query artifact
- **WHEN** the context starts
- **THEN** the `QueryFactory` bean is built and the artifact is never opened

### Requirement: Optional PersistedQueryModel Bean
`JudoModelConfiguration` SHALL expose an optional `PersistedQueryModel` bean from the `JudoModelLoader`, present only when a query artifact is located. It SHALL expose no second `ExpressionModel` bean, and applications defining their own model beans SHALL get no such bean and build the query model.

#### Scenario: Artifact present
- **GIVEN** a valid query artifact on the classpath
- **WHEN** the context starts
- **THEN** one `PersistedQueryModel` bean exists, exactly one `ExpressionModel` bean exists, and the `QueryFactory` bean is restored with an INFO line

#### Scenario: Application with its own model beans
- **GIVEN** an application that defines `AsmModel`, `MeasureModel` and `ExpressionModel` beans itself
- **WHEN** the context starts
- **THEN** no `PersistedQueryModel` bean exists and the `QueryFactory` bean is built

## MODIFIED Requirements

### Requirement: Core Service Bean Definitions
`JudoDefaultSpringConfiguration` SHALL define `@Bean` methods for all core JUDO runtime services, wiring them via `@Autowired` dependencies.

#### Scenario: DAO bean is created
- **GIVEN** a Spring context with `JudoDefaultSpringConfiguration` active and all required dependencies available
- **WHEN** the application context initializes
- **THEN** a `DAO` bean (backed by `RdbmsDAOImpl`) SHALL be created with autowired `DataSource`, `Context`, `AsmModel`, `IdentifierProvider`, `InstanceCollector`, `MetricsCollector`, `SelectStatementExecutor`, `ModifyStatementExecutor`, and `QueryFactory`

#### Scenario: Dispatcher bean is created
- **GIVEN** all required beans are available
- **WHEN** the application context initializes
- **THEN** a `Dispatcher` bean (backed by `DefaultDispatcher`) SHALL be created with all required dependencies including `DAO`, `AccessManager`, `IdentifierSigner`, `ActorResolver`, `PayloadValidator`, and `ValidatorProvider`

#### Scenario: QueryFactory bean is created with JQL expression extraction
- **GIVEN** an `AsmModel` and `MeasureModel` available in the context, and no restorable `PersistedQueryModel`
- **WHEN** `getQueryFactory()` is invoked
- **THEN** a `QueryFactory` SHALL be created using `AsmJqlExtractor` to extract expressions from the ASM model

#### Scenario: QueryFactory bean is restored from a valid artifact
- **GIVEN** an `AsmModel`, `MeasureModel` and `ExpressionModel` in the context and a valid `PersistedQueryModel` bean
- **WHEN** `getQueryFactory()` is invoked through `QueryModelLoadOrBuild`
- **THEN** the `QueryFactory` SHALL be restored without JQL extraction for the query model

### Requirement: Model Bean Extraction
`JudoModelConfiguration` SHALL extract individual model beans from a `JudoModelLoader` instance.

#### Scenario: Individual model beans are available
- **GIVEN** a `JudoModelLoader` bean in the Spring context
- **WHEN** `JudoModelConfiguration` is processed
- **THEN** `AsmModel`, `RdbmsModel`, `ExpressionModel`, `MeasureModel`, and `Asm2RdbmsTransformationTrace` SHALL each be available as individual `@Bean` instances

#### Scenario: Single ExpressionModel bean
- **GIVEN** a valid query artifact on the classpath
- **WHEN** `JudoModelConfiguration` is processed
- **THEN** exactly one `ExpressionModel` bean exists, the same instance as the loader's `-expression.model`

### Requirement: Model Loading from Multiple Sources
`JudoModelLoader` (Spring variant) SHALL support loading models from classpath and filesystem directory with dialect-specific RDBMS model selection. While loading it SHALL compute the SHA-256 digest of the complete bytes of the ASM, measure and expression model files and build a `PersistedQueryModel` locator for `<name>-query.model` without reading that file.

#### Scenario: Load from classpath
- **GIVEN** model files in the classpath under `model/` directory
- **WHEN** `JudoModelLoader.loadFromClassloader(modelName, classLoader, dialect, validate)` is called
- **THEN** ASM, RDBMS, Measure, Expression, Liquibase models, and ASM-to-RDBMS transformation trace SHALL be loaded

#### Scenario: Load from directory
- **GIVEN** a valid directory with model files
- **WHEN** `JudoModelLoader.loadFromDirectory(modelName, directory, dialect)` is called
- **THEN** all models SHALL be loaded from the directory

#### Scenario: Digests and locator
- **GIVEN** model files and a query artifact in a directory or on the classpath
- **WHEN** the loader loads the models
- **THEN** the three digests are recorded, the locator is present, and the query file has not been opened
