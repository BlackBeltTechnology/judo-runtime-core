# Guice Core DI Module Specification

## Purpose
Provides the primary Google Guice dependency injection module (`JudoDefaultModule`) that wires together all JUDO Runtime Core services including DAO, dispatcher, security, expression evaluation, validation, and metrics collection with configurable defaults and override capability.

## Architecture
- **JudoDefaultModule** (`hu.blackbelt.judo.runtime.core.guice.JudoDefaultModule`): Central `AbstractModule` that orchestrates all bindings via the builder pattern and `JudoDefaultModuleConfiguration`.
- **JudoDefaultModuleConfiguration** (`hu.blackbelt.judo.runtime.core.guice.JudoDefaultModuleConfiguration`): Lombok `@Builder` configuration holding all tunable parameters with sensible defaults.
- **JudoModelLoader** (`hu.blackbelt.judo.runtime.core.guice.JudoModelLoader`): Loads ASM, RDBMS, Measure, Expression, Liquibase, and Keycloak metamodels from classpath, directory, or URL.
- **JudoConfigurationQualifiers** (`hu.blackbelt.judo.runtime.core.guice.JudoConfigurationQualifiers`): Binding annotations for injecting configuration values (e.g., `@RdbmsDaoOptimisticLockEnabled`, `@IdentifierSignerSecret`).
- **ComponentScanModule** (`hu.blackbelt.judo.runtime.core.guice.ComponentScanModule`): Reflections-based classpath scanning module for annotation-driven binding.
- **Provider classes** in sub-packages (`core`, `dao.rdbms`, `dispatcher`, `accessmanager`, `security`): Guice `Provider` implementations for each service.

### Key Provider Classes
| Provider | Binds | Scope |
|----------|-------|-------|
| `DataTypeManagerProvider` | `DataTypeManager` | Singleton |
| `UUIDIdentifierProviderProvider` | `IdentifierProvider` | Singleton |
| `ExtendableCoercererProvider` | `ExtendableCoercer` | Eager Singleton |
| `CoercererProvider` | `Coercer` | Singleton |
| `RdbmsResolverProvider` | `RdbmsResolver` | Singleton |
| `RdbmsBuilderProvider` | `RdbmsBuilder` | Singleton |
| `QueryFactoryProvider` | `QueryFactory` | Singleton |
| `SelectStatementExecutorProvider` | `SelectStatementExecutor` | Singleton |
| `ModifyStatementExecutorProvider` | `ModifyStatementExecutor` | Singleton |
| `RdbmsDAOProvider` | `DAO` | Singleton |
| `RdbmsInstanceCollectorProvider` | `InstanceCollector` | Singleton |
| `TransformationTraceServiceProvider` | `TransformationTraceService` | Singleton |
| `PlatformTransactionManagerProvider` | `PlatformTransactionManager` | Singleton |
| `SimpleLiquibaseExecutorProvider` | `SimpleLiquibaseExecutor` | Singleton |
| `DefaultDispatcherProvider` | `Dispatcher` | Eager Singleton |
| `DefaultActorResolverProvider` | `ActorResolver` | Singleton |
| `DefaultIdentifierSignerProvider` | `IdentifierSigner` | Singleton |
| `DefaultMetricsCollectorProvider` | `MetricsCollector` | Singleton |
| `DefaultPayloadValidatorProvider` | `PayloadValidator` | Singleton |
| `DefaultVariableResolverProvider` | `VariableResolver` | Singleton |
| `DispatcherFunctionProviderProvider` | `DispatcherFunctionProvider` | Singleton |
| `OperationCallInterceptorProviderProvider` | `OperationCallInterceptorProvider` | Singleton |
| `ValidatorProviderProvider` | `ValidatorProvider` | Singleton |
| `ThreadContextProvider` | `Context` | Singleton |
| `DefaultAccessManagerProvider` | `AccessManager` | Eager Singleton |
| `DefaultAuthenticationInterceptorProviderProvider` | `AuthenticationInterceptorProvider` | Default |
| `NoPasswordPolicyProvider` | `PasswordPolicy` | - |
| `PathInfoRealmExtractorProvider` | `RealmExtractor` | - |

## Requirements

### Requirement: Model Binding
The module SHALL bind all JUDO metamodels (AsmModel, RdbmsModel, MeasureModel, ExpressionModel, LiquibaseModel) as instances loaded from `JudoModelLoader`, and conditionally bind `KeycloakModel` when present.

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

### Requirement: Configuration Qualifier Binding
The module SHALL bind all configuration parameters via `@BindingAnnotation`-qualified annotations from `JudoConfigurationQualifiers`, using values from `JudoDefaultModuleConfiguration` with documented defaults.

#### Scenario: Default configuration values are bound
- **GIVEN** a `JudoDefaultModule` built with no explicit configuration overrides
- **WHEN** the injector is created
- **THEN** `@RdbmsDaoOptimisticLockEnabled` SHALL resolve to `true`, `@RdbmsDaoChunkSize` to `1000`, `@DispatcherEnableDefaultValidation` to `true`, `@MetricsCollectorEnabled` to `false`, `@ThreadContextInheritableContext` to `true`, and `@RdbmsSequenceStart` to `1L`

#### Scenario: IdentifierSignerSecret is auto-generated when null
- **GIVEN** `JudoDefaultModuleConfiguration` with `identifierSignerSecret` set to null
- **WHEN** `configureOptions()` is invoked in `JudoDefaultModule`
- **THEN** a Base64-encoded 1024-bit secret SHALL be generated via `generateNewSecret()` using `SecureRandom.getInstanceStrong()`

### Requirement: Service Override Pattern
Each service binding in `JudoDefaultModule` SHALL support instance override: if a non-null instance is provided in `JudoDefaultModuleConfiguration`, it SHALL be bound directly; otherwise, the corresponding Guice `Provider` SHALL be used.

#### Scenario: Custom DAO instance overrides default provider
- **GIVEN** a `JudoDefaultModuleConfiguration` with a non-null `dao` field
- **WHEN** `configureDAO()` is invoked
- **THEN** `DAO` SHALL be bound to the provided instance, not via `RdbmsDAOProvider`

#### Scenario: Default provider is used when no override is given
- **GIVEN** a `JudoDefaultModuleConfiguration` with `dao` set to null
- **WHEN** `configureDAO()` is invoked
- **THEN** `DAO` SHALL be bound via `RdbmsDAOProvider` in Singleton scope

### Requirement: Model Loading from Multiple Sources
`JudoModelLoader` SHALL support loading JUDO models from classpath, filesystem directory, and URL, selecting the correct dialect-specific RDBMS model file.

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

### Requirement: Component Scanning
`ComponentScanModule` SHALL use the Reflections library to scan a given package for classes annotated with specified annotations and bind them in Guice.

#### Scenario: Annotated classes are discovered and bound
- **GIVEN** a `ComponentScanModule` initialized with a package name and one or more annotation classes
- **WHEN** `configure()` is invoked
- **THEN** all classes in the package annotated with any of the specified annotations SHALL be bound in the Guice module

### Requirement: Export Fallback
When no `Export` implementation is provided, the module SHALL bind `Export` to `UnsupportedExportImpl`.

#### Scenario: Default export binding
- **GIVEN** a `JudoDefaultModuleConfiguration` with `export` set to null
- **WHEN** `configureExport()` is invoked
- **THEN** `Export` SHALL be bound to `UnsupportedExportImpl`

### Requirement: Builder Pattern Module Construction
`JudoDefaultModule` SHALL support construction via both a direct `JudoDefaultModuleConfiguration` object and individual builder parameters, where providing a `configuration` object takes precedence.

#### Scenario: Configuration object takes precedence over individual parameters
- **GIVEN** a `JudoDefaultModule.builder()` call with both a `configuration` object and individual parameter `rdbmsDaoChunkSize(500)`
- **WHEN** the module is built
- **THEN** the configuration from the `configuration` object SHALL be used, ignoring the individual parameter
