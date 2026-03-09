# Spring Autoconfiguration Module Specification

## Purpose
Provides Spring Boot autoconfiguration for the JUDO Runtime Core, wiring all core services (DAO, dispatcher, security, expression evaluation, validation, metrics) as Spring `@Bean` definitions with `@Autowired` dependency injection and `@Configuration` classes.

## Architecture
- **JudoDefaultSpringConfiguration** (`hu.blackbelt.judo.runtime.core.spring.JudoDefaultSpringConfiguration`): Primary `@Configuration` class defining `@Bean` methods for DAO, QueryFactory, RdbmsBuilder, SelectStatementExecutor, ModifyStatementExecutor, InstanceCollector, RdbmsResolver, TransformationTraceService, ActorResolver, ValidatorProvider, PayloadValidator, Dispatcher, IdentifierSigner, and VariableResolver.
- **JudoBaseServiceConfiguration** (`hu.blackbelt.judo.runtime.core.spring.JudoBaseServiceConfiguration`): `@Configuration` class providing foundational beans: `Coercer` (DefaultCoercer), `DataTypeManager`, `IdentifierProvider` (UUIDIdentifierProvider), `MetricsCollector`, `AuthenticationInterceptorProvider`, `OperationCallInterceptorProvider`, `DispatcherFunctionProvider`, `Context` (ThreadContext), `PasswordPolicy` (NoPasswordPolicy), and `Export` (UnsupportedExportImpl).
- **JudoModelConfiguration** (`hu.blackbelt.judo.runtime.core.spring.JudoModelConfiguration`): `@Configuration` class that extracts individual model beans (`AsmModel`, `RdbmsModel`, `ExpressionModel`, `MeasureModel`, `Asm2RdbmsTransformationTrace`) from `JudoModelLoader`.
- **JudoModelLoaderConfiguration** (`hu.blackbelt.judo.runtime.core.spring.JudoModelLoaderConfiguration`): `@Configuration` class for loading the `JudoModelLoader` bean.
- **JudoModelLoader** (`hu.blackbelt.judo.runtime.core.spring.JudoModelLoader`): Model loading utility supporting classpath, directory, and URL loading (similar to the Guice variant but without Keycloak support).
- **JudoDataSourceCondition** (`hu.blackbelt.judo.runtime.core.spring.JudoDataSourceCondition`): Spring `Condition` for conditional DataSource bean creation.
- **AntlrCheckConfiguration** / **AntlrCheckBeanRegistration** / **AntlrRuntimeIncompatibilityException**: ANTLR runtime version compatibility checking to detect and report version mismatches at startup.

## Requirements

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
- **GIVEN** an `AsmModel` and `MeasureModel` available in the context
- **WHEN** `getQueryFactory()` is invoked
- **THEN** a `QueryFactory` SHALL be created using `AsmJqlExtractor` to extract expressions from the ASM model

### Requirement: Foundational Service Beans
`JudoBaseServiceConfiguration` SHALL provide default implementations of foundational services.

#### Scenario: Default Coercer is provided
- **GIVEN** a Spring context with `JudoBaseServiceConfiguration` active
- **WHEN** the context initializes
- **THEN** a `Coercer` bean backed by `DefaultCoercer` SHALL be available

#### Scenario: UUIDIdentifierProvider is the default
- **GIVEN** a Spring context with `JudoBaseServiceConfiguration` active
- **WHEN** the context initializes
- **THEN** an `IdentifierProvider` bean backed by `UUIDIdentifierProvider` SHALL be available

#### Scenario: ThreadContext is configured as Context
- **GIVEN** a Spring context with `JudoBaseServiceConfiguration` active
- **WHEN** the context initializes
- **THEN** a `Context` bean backed by `ThreadContext` SHALL be created with `debugThreadFork=false` and `inheritableContext=true`

#### Scenario: NoPasswordPolicy is the default
- **GIVEN** a Spring context without Keycloak integration
- **WHEN** the context initializes
- **THEN** a `PasswordPolicy` bean backed by `NoPasswordPolicy` SHALL be available

#### Scenario: UnsupportedExportImpl is the default export
- **GIVEN** a Spring context without explicit export configuration
- **WHEN** the context initializes
- **THEN** an `Export` bean backed by `UnsupportedExportImpl` SHALL be available

### Requirement: Model Bean Extraction
`JudoModelConfiguration` SHALL extract individual model beans from a `JudoModelLoader` instance.

#### Scenario: Individual model beans are available
- **GIVEN** a `JudoModelLoader` bean in the Spring context
- **WHEN** `JudoModelConfiguration` is processed
- **THEN** `AsmModel`, `RdbmsModel`, `ExpressionModel`, `MeasureModel`, and `Asm2RdbmsTransformationTrace` SHALL each be available as individual `@Bean` instances

### Requirement: Model Loading from Multiple Sources
`JudoModelLoader` (Spring variant) SHALL support loading models from classpath and filesystem directory with dialect-specific RDBMS model selection.

#### Scenario: Load from classpath
- **GIVEN** model files in the classpath under `model/` directory
- **WHEN** `JudoModelLoader.loadFromClassloader(modelName, classLoader, dialect, validate)` is called
- **THEN** ASM, RDBMS, Measure, Expression, Liquibase models, and ASM-to-RDBMS transformation trace SHALL be loaded

#### Scenario: Load from directory
- **GIVEN** a valid directory with model files
- **WHEN** `JudoModelLoader.loadFromDirectory(modelName, directory, dialect)` is called
- **THEN** all models SHALL be loaded from the directory

### Requirement: VariableResolver Configuration
`JudoDefaultSpringConfiguration` SHALL create a `VariableResolver` bean with pre-registered system variable suppliers and environment/sequence/request functions.

#### Scenario: System variables are registered
- **GIVEN** a Spring context with `JudoDefaultSpringConfiguration`
- **WHEN** `getVariableResolver()` is invoked
- **THEN** the `DefaultVariableResolver` SHALL have `SYSTEM.current_timestamp`, `SYSTEM.current_date`, `SYSTEM.current_time` suppliers registered, plus `ENVIRONMENT`, `SEQUENCE`, and `REQUEST` function providers

### Requirement: ANTLR Runtime Compatibility Check
The module SHALL detect ANTLR runtime version incompatibilities at startup and throw `AntlrRuntimeIncompatibilityException` if the runtime version does not match the expected version.

#### Scenario: Incompatible ANTLR version detected
- **GIVEN** an ANTLR runtime on the classpath with a version different from the expected version
- **WHEN** `AntlrCheckConfiguration` processes
- **THEN** an `AntlrRuntimeIncompatibilityException` SHALL be thrown with a message indicating the version mismatch

### Requirement: Optional External Service Integration
`JudoDefaultSpringConfiguration` SHALL support optional integration with `OpenIdConfigurationProvider`, `TokenIssuer`, and `TokenValidator` via `@Autowired(required = false)`.

#### Scenario: Dispatcher works without OpenID provider
- **GIVEN** no `OpenIdConfigurationProvider` bean in the context
- **WHEN** the `Dispatcher` bean is created
- **THEN** the dispatcher SHALL be created with `openIdConfigurationProvider` set to null, and authentication features requiring OpenID SHALL be disabled
