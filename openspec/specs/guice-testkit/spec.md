# Guice Testkit Module Specification

## Purpose
Provides a comprehensive testing toolkit for JUDO Runtime Core applications using Google Guice, including JUnit 5 extensions, datasource fixtures with TestContainers support, runtime fixtures for model loading and injector management, transaction management, custom annotation-driven test configuration, and interceptor testing support.

## Architecture
- **JudoRuntimeFixture** (`hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture`): Core test fixture managing model loading, Guice injector creation, transaction lifecycle (begin/commit/rollback/savepoint), and interceptor registration with deferred dependency injection.
- **JudoDatasourceFixture** (`hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceFixture`): Manages database lifecycle including HSQLDB in-memory, PostgreSQL via TestContainers, and YugabyteDB. Provides DataSource creation, table truncation, table dropping, and transactional test helpers.
- **JudoRuntimeExtension** (`hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeExtension`): JUnit 5 extension implementing `BeforeAllCallback`, `AfterAllCallback`, `BeforeEachCallback`, `AfterEachCallback`, and `ParameterResolver` for automated test lifecycle management.
- **JudoTest** (`hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest`): Custom annotation combining `@Test` with `@ExtendWith(JudoTestExtension.class)`, providing declarative test configuration for model name, dialect, container, transaction handling, model source, custom modules, datasource mode, and interceptors.
- **JudoTestExtension** (`hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTestExtension`): JUnit 5 extension that processes `@JudoTest` annotations and manages test lifecycle.
- **JudoDatasourceByClassExtension** / **JudoDatasourceSingletonExtension**: Extensions for shared datasource lifecycle across test classes.
- **JudoRuntimeByClassExtension**: Extension for class-level runtime fixture sharing.
- **TestOperationCallInterceptorProvider** (`hu.blackbelt.judo.runtime.core.guice.testkit.util.TestOperationCallInterceptorProvider`): Mutable interceptor provider for testing, allowing runtime add/remove/clear of interceptors.
- **ReferenceInjector** (`hu.blackbelt.judo.runtime.core.guice.testkit.util.ReferenceInjector`): Utility for injecting Guice-managed dependencies into objects using `@Reference` annotations.
- **EnvironmentVariableMocker** / **EnvironmentVariables**: Utilities for mocking environment variables in tests.
- **YugabytedbSQLContainer**: TestContainers wrapper for YugabyteDB.

## Requirements

### Requirement: Model Loading with Auto-Detection
`JudoRuntimeFixture.prepare()` SHALL support loading JUDO models via three strategies: AUTO (filesystem first, fallback to classpath), FILESYSTEM-only, and CLASSPATH-only.

#### Scenario: Auto model loading with filesystem fallback to classpath
- **GIVEN** a `JudoRuntimeFixture` and a model name not present on the filesystem
- **WHEN** `prepare(modelName, dataSource, dialect, ModelSource.AUTO)` is called
- **THEN** the fixture SHALL attempt filesystem loading from `target/generated-test-sources/model`, and upon failure, fall back to classpath loading

#### Scenario: Filesystem-only loading fails when model not on disk
- **GIVEN** a `JudoRuntimeFixture` and a model name not present on the filesystem
- **WHEN** `prepare(modelName, dataSource, dialect, ModelSource.FILESYSTEM)` is called
- **THEN** an exception SHALL be thrown

### Requirement: Guice Injector Creation and Module Composition
`JudoRuntimeFixture.init()` SHALL create a Guice injector by combining the user-provided module, `JudoDefaultModule` (with model and configuration), and the database module.

#### Scenario: Injector is created with all modules
- **GIVEN** a prepared `JudoRuntimeFixture` with a valid model
- **WHEN** `init(module, injectModulesTo)` is called
- **THEN** a Guice `Injector` SHALL be created combining the custom module, `JudoDefaultModule`, and the database module, and SHALL be accessible via `getInjector()`

### Requirement: Transaction Lifecycle Management
`JudoRuntimeFixture` SHALL provide methods for managing transactions: `beginTransaction()`, `commitTransaction()`, `rollbackTransaction()`, `createSavePoint()`, and `rollbackToSavePoint()`.

#### Scenario: Transaction begin and rollback
- **GIVEN** an initialized `JudoRuntimeFixture`
- **WHEN** `beginTransaction()` is called followed by `rollbackTransaction()`
- **THEN** the transaction SHALL be rolled back and the database state SHALL revert

#### Scenario: Duplicate begin transaction throws exception
- **GIVEN** an active transaction
- **WHEN** `beginTransaction()` is called again without completing the first transaction
- **THEN** an `IllegalStateException` SHALL be thrown

#### Scenario: Savepoint support
- **GIVEN** an active transaction
- **WHEN** `createSavePoint()` is called, some operations are performed, then `rollbackToSavePoint(savePoint)` is called
- **THEN** the database state SHALL revert to the savepoint

### Requirement: Multi-Database Support via JudoDatasourceFixture
`JudoDatasourceFixture` SHALL support HSQLDB in-memory, PostgreSQL via TestContainers, and YugabyteDB via TestContainers, selected by dialect and container configuration.

#### Scenario: HSQLDB in-memory datasource
- **GIVEN** dialect set to `"hsqldb"`
- **WHEN** `prepareDatasources()` is called
- **THEN** an HSQLDB `JDBCDataSource` SHALL be created with a unique in-memory URL

#### Scenario: PostgreSQL TestContainers datasource
- **GIVEN** dialect set to `"postgresql"` and container set to `"postgresql"`
- **WHEN** `setupDatabase()` and `prepareDatasources()` are called
- **THEN** a PostgreSQL TestContainer SHALL be started and a `PGSimpleDataSource` SHALL be created with the container's JDBC URL

#### Scenario: Table truncation between tests
- **GIVEN** an initialized datasource and an `RdbmsModel` with tables
- **WHEN** `truncateTables(rdbmsModel)` is called
- **THEN** all RDBMS tables SHALL be truncated using the dialect-appropriate SQL statement

### Requirement: @JudoTest Annotation-Driven Configuration
The `@JudoTest` annotation SHALL provide declarative configuration for model name, dialect, container, transaction handling, model source, custom Guice modules, datasource mode, and interceptors.

#### Scenario: Default @JudoTest configuration
- **GIVEN** a test method annotated with `@JudoTest`
- **WHEN** the test is executed
- **THEN** the model name SHALL default to `"example"`, dialect to `"hsqldb"`, transaction handling to `AUTO_ROLLBACK`, model source to `AUTO`, and datasource mode to `BY_METHOD`

#### Scenario: Environment variable overrides annotation dialect
- **GIVEN** a test annotated with `@JudoTest(dialect="hsqldb")` and environment variable `JUDO_TEST_DIALECT=postgresql`
- **WHEN** the test is executed
- **THEN** the dialect SHALL be `"postgresql"` (environment variable takes precedence)

#### Scenario: Transaction handling AUTO_ROLLBACK
- **GIVEN** `@JudoTest(transaction = TransactionHandling.AUTO_ROLLBACK)`
- **WHEN** the test completes
- **THEN** the transaction SHALL be automatically rolled back, keeping the database clean

#### Scenario: Transaction handling AUTO_COMMIT with table truncation
- **GIVEN** `@JudoTest(transaction = TransactionHandling.AUTO_COMMIT, truncateTables = true)`
- **WHEN** the test completes
- **THEN** the transaction SHALL be committed and all tables SHALL be truncated

### Requirement: Interceptor Registration and Deferred Injection
`JudoRuntimeFixture` SHALL support registering `OperationCallInterceptor` classes and instances before `init()`, with automatic deferred dependency injection via `ReferenceInjector` after the Guice injector is created.

#### Scenario: Register interceptor class before init
- **GIVEN** an un-initialized `JudoRuntimeFixture`
- **WHEN** `addInterceptor(MyInterceptor.class)` is called, then `init()` is called
- **THEN** `MyInterceptor` SHALL be instantiated via its no-arg constructor, registered in the `TestOperationCallInterceptorProvider`, and have its `@Reference` dependencies injected from the Guice injector

#### Scenario: Register interceptor instance before init
- **GIVEN** a pre-configured `OperationCallInterceptor` instance
- **WHEN** `addInterceptor(instance)` is called, then `init()` is called
- **THEN** the instance SHALL be registered in the `TestOperationCallInterceptorProvider` and have its dependencies injected

#### Scenario: Cannot add interceptor after init
- **GIVEN** an initialized `JudoRuntimeFixture`
- **WHEN** `addInterceptor(SomeInterceptor.class)` is called
- **THEN** an `IllegalStateException` SHALL be thrown

### Requirement: JUnit 5 Extension Lifecycle
`JudoRuntimeExtension` SHALL implement `BeforeAllCallback`, `AfterAllCallback`, `BeforeEachCallback`, and `AfterEachCallback` to automate datasource setup, model loading, injector creation, transaction management, and table cleanup.

#### Scenario: Full test lifecycle
- **GIVEN** a test class using `JudoRuntimeExtension`
- **WHEN** the test suite runs
- **THEN** `beforeAll` SHALL set up the database and load the model, `beforeEach` SHALL create the injector and begin a transaction, `afterEach` SHALL commit the transaction and truncate tables, and `afterAll` SHALL tear down the datasource

### Requirement: Datasource Lifecycle Modes
The `@JudoTest` annotation SHALL support three datasource modes: `BY_METHOD` (new datasource per test), `BY_CLASS` (shared per class), and `SINGLETON` (shared globally).

#### Scenario: BY_METHOD datasource mode
- **GIVEN** `@JudoTest(dataSourceMode = DataSourceMode.BY_METHOD)`
- **WHEN** multiple test methods run
- **THEN** each test method SHALL get a fresh datasource for maximum isolation

#### Scenario: SINGLETON datasource mode
- **GIVEN** `@JudoTest(dataSourceMode = DataSourceMode.SINGLETON)`
- **WHEN** multiple test classes run
- **THEN** a single shared datasource SHALL be used across all test classes

### Requirement: Transactional Test Helpers
`JudoDatasourceFixture` SHALL provide `assertThrowsInTransaction()` and `runInTransaction()` utility methods for executing code within transaction boundaries.

#### Scenario: assertThrowsInTransaction rolls back on exception
- **GIVEN** an initialized `JudoDatasourceFixture`
- **WHEN** `assertThrowsInTransaction(SomeException.class, executable)` is called and the executable throws `SomeException`
- **THEN** the transaction SHALL be rolled back and the assertion SHALL pass

#### Scenario: runInTransaction commits on success
- **GIVEN** an initialized `JudoDatasourceFixture`
- **WHEN** `runInTransaction(supplier)` is called and the supplier returns successfully
- **THEN** the transaction SHALL be committed and the result SHALL be returned
