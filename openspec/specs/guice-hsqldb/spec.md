# Guice HSQLDB Module Specification

## Purpose
Provides a Google Guice module (`JudoHsqldbModule`) that configures HSQLDB as the database backend for the JUDO runtime, including DataSource, dialect, parameter mapper, sequence provider, schema initialization, and optional embedded HSQLDB server.

## Architecture
- **JudoHsqldbModule** (`hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb.JudoHsqldbModule`): `AbstractModule` providing HSQLDB-specific Guice bindings with builder pattern and `JudoHsqldbModuleConfiguration`.
- **JudoHsqldbModuleConfiguration** (`hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb.JudoHsqldbModuleConfiguration`): Lombok `@Builder` configuration with defaults for HSQLDB (port 31001, database name "judo", run server false).
- **HsqlDbConfigurationQualifier** (`hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb.HsqlDbConfigurationQualifier`): Binding annotations for HSQLDB-specific configuration (port, database name, database path).
- **HsqldbDataSourceProvider**: Provides `DataSource` for HSQLDB connections.
- **HsqldbMapperFactoryProvider**: Provides `MapperFactory` with HSQLDB-specific query mappers.
- **HsqldbRdbmsInitProvider**: Provides `RdbmsInit` for HSQLDB schema initialization via Liquibase.
- **HsqldbRdbmsParameterMapperProvider**: Provides `RdbmsParameterMapper` for HSQLDB parameter binding.
- **HsqldbRdbmsSequenceProvider**: Provides `Sequence` for HSQLDB sequence management.
- **HsqldbServerProvider**: Provides an optional embedded HSQLDB `Server` instance.

## Requirements

### Requirement: HSQLDB Dialect Binding
The module SHALL bind `Dialect` to a `HsqldbDialect` instance.

#### Scenario: HsqldbDialect is always bound
- **GIVEN** a `JudoHsqldbModule` is installed in the Guice injector
- **WHEN** the module's `configure()` method executes
- **THEN** `Dialect` SHALL be bound to a new `HsqldbDialect` instance

### Requirement: DataSource Provider with Override
The module SHALL provide an HSQLDB `DataSource` via `HsqldbDataSourceProvider` or accept a pre-configured `DataSource` instance.

#### Scenario: Default DataSource via provider
- **GIVEN** `JudoHsqldbModuleConfiguration` with `dataSource` set to null
- **WHEN** `configureDataSource()` is invoked
- **THEN** `DataSource` SHALL be bound via `HsqldbDataSourceProvider` in Singleton scope

#### Scenario: Custom DataSource instance
- **GIVEN** `JudoHsqldbModuleConfiguration` with a non-null `dataSource`
- **WHEN** `configureDataSource()` is invoked
- **THEN** `DataSource` SHALL be bound to the provided instance directly

### Requirement: Embedded HSQLDB Server
The module SHALL conditionally start an embedded HSQLDB `Server` based on the `runServer` configuration flag.

#### Scenario: Server started when runServer is true
- **GIVEN** `JudoHsqldbModuleConfiguration` with `runServer` set to `true`
- **WHEN** `configureServer()` is invoked
- **THEN** `Server` SHALL be bound via `HsqldbServerProvider` in Singleton scope

#### Scenario: Server not started when runServer is false
- **GIVEN** `JudoHsqldbModuleConfiguration` with `runServer` set to `false` (the default)
- **WHEN** `configureServer()` is invoked
- **THEN** `Server` SHALL be bound to a null provider in Singleton scope

### Requirement: RDBMS Initialization
The module SHALL bind `RdbmsInit` for HSQLDB schema setup via Liquibase.

#### Scenario: Default RdbmsInit via provider
- **GIVEN** `JudoHsqldbModuleConfiguration` with `rdbmsInit` set to null
- **WHEN** `configureRdbmsInit()` is invoked
- **THEN** `RdbmsInit` SHALL be bound via `HsqldbRdbmsInitProvider` in Singleton scope

### Requirement: Sequence Provider
The module SHALL bind `Sequence` for HSQLDB-based sequence generation.

#### Scenario: Default Sequence via provider
- **GIVEN** `JudoHsqldbModuleConfiguration` with `sequence` set to null
- **WHEN** `configureSequence()` is invoked
- **THEN** `Sequence` SHALL be bound via `HsqldbRdbmsSequenceProvider` in Singleton scope

### Requirement: Query Mapper Factory
The module SHALL bind `MapperFactory` with HSQLDB-specific SQL query mappers.

#### Scenario: Default MapperFactory via provider
- **GIVEN** `JudoHsqldbModuleConfiguration` with `mapperFactory` set to null
- **WHEN** `configureMapperFactory()` is invoked
- **THEN** `MapperFactory` SHALL be bound via `HsqldbMapperFactoryProvider` in Singleton scope

### Requirement: RDBMS Parameter Mapper
The module SHALL bind `RdbmsParameterMapper` with HSQLDB-specific parameter handling.

#### Scenario: Default RdbmsParameterMapper via provider
- **GIVEN** `JudoHsqldbModuleConfiguration` with `rdbmsParameterMapper` set to null
- **WHEN** `configureRdbmsParameterMapper()` is invoked
- **THEN** `RdbmsParameterMapper` SHALL be bound via `HsqldbRdbmsParameterMapperProvider` in Singleton scope

### Requirement: Default Configuration Values
The module SHALL provide sensible default values for HSQLDB configuration.

#### Scenario: Default configuration
- **GIVEN** a `JudoHsqldbModuleConfiguration` built with no overrides
- **WHEN** default values are inspected
- **THEN** `runServer` SHALL be `false`, `databaseName` SHALL be `"judo"`, `port` SHALL be `31001`, and `databasePath` SHALL point to `"./judo.db"`
