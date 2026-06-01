## ADDED Requirements

### Requirement: Class-Level Runtime Caching for BY_CLASS Mode
`JudoTestExtension` SHALL cache the derived runtime artifacts (`QueryFactory`, database `Module`, `SimpleLiquibaseExecutor`, Guice `Injector`, and `PlatformTransactionManager`) at the class scope for `@JudoTest(dataSourceMode = DataSourceMode.BY_CLASS)` and SHALL reuse them across every test method of the class.

#### Scenario: Same Guice injector is returned for every method in a BY_CLASS class
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS)` with multiple test methods
- **WHEN** the test methods execute in any order
- **THEN** `JudoRuntimeFixture#getInjector()` SHALL return the SAME `Injector` instance for every method (reference identity)

#### Scenario: Same QueryFactory is reused across methods in a BY_CLASS class
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS)` with multiple test methods
- **WHEN** the test methods execute
- **THEN** the `QueryFactory` exposed via `JudoRuntimeFixture` SHALL be the SAME instance across methods

#### Scenario: Same PlatformTransactionManager is reused across methods in a BY_CLASS class
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS)` with multiple test methods
- **WHEN** the test methods execute
- **THEN** the `PlatformTransactionManager` exposed via `JudoRuntimeFixture` SHALL be the SAME instance across methods

### Requirement: JVM-Level Runtime Caching for SINGLETON Mode
`JudoTestExtension` SHALL cache derived runtime artifacts for `@JudoTest(dataSourceMode = DataSourceMode.SINGLETON)` in a JVM-scoped map keyed by `(modelName, dialect, modelSource, modules, interceptors)` and SHALL reuse them across all test classes that resolve to the same key.

#### Scenario: Singleton injector is reused across two SINGLETON-annotated classes with identical configuration
- **GIVEN** two test classes A and B both annotated `@JudoTest(dataSourceMode = SINGLETON, modelName = "x", ...)` with identical `modules` and `interceptors`
- **WHEN** classes A and B run in the same JVM
- **THEN** `JudoRuntimeFixture#getInjector()` SHALL return the SAME `Injector` instance in both classes

#### Scenario: Singleton cache distinguishes by modules and interceptors
- **GIVEN** two test classes A and B both annotated `@JudoTest(dataSourceMode = SINGLETON, modelName = "x")` but with different `modules` arrays
- **WHEN** both classes run in the same JVM
- **THEN** they SHALL receive DIFFERENT `Injector` instances

### Requirement: Liquibase Executes Exactly Once Per Cached Runtime
For `BY_CLASS` and `SINGLETON` modes, the Liquibase changelog re-validation (`RdbmsInit#execute(datasource)`) SHALL run at most once per cached runtime and SHALL NOT run again on subsequent test methods that share the cached runtime.

#### Scenario: Liquibase is invoked exactly once for a 20-method BY_CLASS class
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS)` with 20 test methods and a counting `SimpleLiquibaseExecutor`
- **WHEN** all 20 methods execute
- **THEN** the counting executor SHALL report exactly 1 invocation of `execute(...)`

### Requirement: Cached Runtime Resources Are Closed Exactly Once
A cached runtime SHALL be wrapped as `ExtensionContext.Store.CloseableResource` (or registered for equivalent JVM-shutdown handling for SINGLETON) so that its `Injector` and the underlying connection pool SHALL be closed exactly once when the owning scope ends.

#### Scenario: BY_CLASS cache is closed when the class finishes
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS)` whose test methods completed
- **WHEN** JUnit cleans up the class-scoped store
- **THEN** the cached `Injector` and HikariCP pool SHALL be closed exactly once and SHALL NOT throw on close

#### Scenario: SINGLETON cache is closed exactly once at JVM end
- **GIVEN** one or more test classes annotated `@JudoTest(dataSourceMode = SINGLETON)`
- **WHEN** the JUnit root store is cleaned up at the end of the test run
- **THEN** every cached `Injector` SHALL be closed exactly once and SHALL NOT throw on close

### Requirement: BY_METHOD Mode Preserves Per-Test Isolation
`JudoTestExtension` SHALL build a fresh `Injector`, `QueryFactory`, and `PlatformTransactionManager` for every test method under `@JudoTest(dataSourceMode = DataSourceMode.BY_METHOD)` and SHALL NOT reuse artifacts cached for `BY_CLASS` or `SINGLETON` mode.

#### Scenario: BY_METHOD methods receive distinct injectors
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_METHOD)` with two methods
- **WHEN** both methods execute
- **THEN** `JudoRuntimeFixture#getInjector()` SHALL return DIFFERENT `Injector` instances for the two methods

#### Scenario: BY_METHOD still calls JudoRuntimeFixture#init(...)
- **GIVEN** a test subclass of `JudoRuntimeFixture` overriding `init(Module, Object)` and a `@JudoTest(dataSourceMode = BY_METHOD)` test method
- **WHEN** the test method executes
- **THEN** the overridden `init(...)` SHALL be invoked exactly once for that method

### Requirement: Cache Key Includes Modules and Interceptors
The cache key for `BY_CLASS` and `SINGLETON` runtime caching SHALL include the `modules()` and `interceptors()` values of the `@JudoTest` annotation in addition to model name, dialect, model source, and (for BY_CLASS) the test class.

#### Scenario: Different modules produce different cache entries
- **GIVEN** two test classes with `@JudoTest(...)` differing only in `modules` arrays
- **WHEN** both classes run
- **THEN** they SHALL NOT share a cached `Injector` instance

### Requirement: SINGLETON Model Loader And Datasource Are Keyed By Configuration
The JVM-wide SINGLETON model loader cache SHALL be keyed by `(modelName, dialect, modelSource)` and the JVM-wide SINGLETON datasource cache SHALL be keyed by `(dialect, container)`. Two `@JudoTest(dataSourceMode = SINGLETON)` test classes whose annotations resolve to different keys SHALL NOT share a `JudoModelLoader` or a `JudoDatasourceFixture`.

This closes the gap where the previous implementation held a single unkeyed `singletonModelLoader` / `singletonDatasource` field — first-write-wins — and silently handed the first SINGLETON class's resources to every subsequent SINGLETON class regardless of their configuration.

#### Scenario: Two SINGLETON classes with different modelName receive different model loaders
- **GIVEN** two test classes A and B annotated `@JudoTest(dataSourceMode = SINGLETON)` differing only in `modelName`
- **WHEN** both classes run in the same JVM
- **THEN** the `JudoModelLoader` exposed to class A SHALL NOT be the same instance as the one exposed to class B

#### Scenario: Two SINGLETON classes with different dialect receive different datasources
- **GIVEN** two test classes A and B annotated `@JudoTest(dataSourceMode = SINGLETON)` differing only in resolved `dialect` (after env-var resolution)
- **WHEN** both classes run in the same JVM
- **THEN** they SHALL receive DIFFERENT `JudoDatasourceFixture` instances

#### Scenario: Two SINGLETON classes with different container receive different datasources
- **GIVEN** two test classes A and B annotated `@JudoTest(dataSourceMode = SINGLETON)` differing only in resolved `container` (after env-var resolution and `postgresql + none` auto-detection)
- **WHEN** both classes run in the same JVM
- **THEN** they SHALL receive DIFFERENT `JudoDatasourceFixture` instances

#### Scenario: All SINGLETON resource maps are cleared at JVM shutdown
- **GIVEN** one or more entries exist in the SINGLETON model loader map and the SINGLETON datasource map
- **WHEN** the JVM shutdown hook (`closeAllSingletonRuntimes`) runs
- **THEN** every cached datasource SHALL have `teardownDatasource()` invoked exactly once
- **AND** every entry in the model loader, datasource, and runtime maps SHALL be cleared so a subsequent test suite in the same JVM starts fresh

### Requirement: Performance Speed-up Guard for BY_CLASS Mode
A slow-tagged regression test SHALL assert that, for a real-world model exercising `BY_CLASS` mode with at least 20 test methods, the average elapsed time of the cached methods MUST be at least 5× shorter than the cold first-method elapsed time.

#### Scenario: 20-method rackinspect BY_CLASS test demonstrates >= 5x speed-up
- **GIVEN** a `@Tag("slow")` test class running `rackinspect` with `BY_CLASS` mode and 20 ordered methods
- **WHEN** all methods complete and timings are recorded around `JudoTestExtension`'s lifecycle
- **THEN** `firstNs / cachedAvgNs` SHALL be at least 5
- **AND** the absolute `cachedAvgNs` SHALL not exceed 6 seconds
