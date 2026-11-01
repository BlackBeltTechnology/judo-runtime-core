## ADDED Requirements

### Requirement: Opt-in JVM-wide sharing of the model-derived runtime for BY_CLASS
When the system property `judo.testkit.shareModel` is `true`, `BY_CLASS` test classes SHALL reuse one model loader and one query factory per `(modelName, dialect, modelSource)` within the JVM, while the datasource, database schema, injector and transaction manager SHALL still be created per test class. When the property is unset or `false`, behaviour SHALL be unchanged.

#### Scenario: Second class reuses the model-derived runtime
- **GIVEN** `judo.testkit.shareModel=true`
- **AND** two test classes annotated `@JudoTest(dataSourceMode = BY_CLASS, shareInjector = true)` with the same model name, dialect and model source run in the same JVM
- **WHEN** the second class starts
- **THEN** it SHALL receive the same `JudoModelLoader` and `QueryFactory` instances as the first class
- **AND** it SHALL receive a different `Injector` instance

#### Scenario: Data isolation between classes is preserved
- **GIVEN** `judo.testkit.shareModel=true`
- **AND** a first `BY_CLASS` class that commits rows
- **WHEN** a second `BY_CLASS` class with the same model configuration queries the same entity type
- **THEN** it SHALL see an empty table

#### Scenario: Distinct model configurations are not shared
- **GIVEN** `judo.testkit.shareModel=true`
- **WHEN** two `BY_CLASS` classes differ in model name, dialect or model source
- **THEN** each SHALL receive its own model loader and query factory

#### Scenario: Property off keeps per-class model loading
- **GIVEN** `judo.testkit.shareModel` is unset
- **WHEN** two `BY_CLASS` classes with the same model configuration run
- **THEN** each class SHALL load its own model loader and build its own query factory
- **AND** the shared model-runtime cache SHALL stay empty

#### Scenario: Other modes are unaffected
- **GIVEN** `judo.testkit.shareModel=true`
- **WHEN** a `BY_METHOD` class or a method-level `@JudoTest` runs
- **THEN** each test method SHALL still load its own model and build its own query factory

#### Scenario: Shared model runtime outlives a launcher session
- **GIVEN** `judo.testkit.shareModel=true`
- **AND** a build tool that runs one JUnit launcher session per test class in a reused JVM (surefire forks)
- **WHEN** the singleton shutdown hook runs at the end of a session
- **THEN** the shared model runtime SHALL be kept, and the next class in the same JVM SHALL reuse it
