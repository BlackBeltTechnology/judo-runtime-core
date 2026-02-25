# Guice PostgreSQL Module Specification

## Purpose
Provides a Google Guice module (`JudoPostgresqlModule`) that configures PostgreSQL as the production database backend for the JUDO runtime, including connection pooling via HikariCP, dialect binding, parameter mapping, sequence management, and schema initialization.

## Architecture
- **JudoPostgresqlModule** (`hu.blackbelt.judo.runtime.core.guice.dao.rdbms.postgresql.JudoPostgresqlModule`): `AbstractModule` providing PostgreSQL-specific Guice bindings with builder pattern and `JudoPostgresqlModuleConfiguration`.
- **JudoPostgresqlModuleConfiguration** (`hu.blackbelt.judo.runtime.core.guice.dao.rdbms.postgresql.JudoPostgresqlModuleConfiguration`): Lombok `@Builder` configuration with defaults (host "localhost", port 5432, user "judo", password "judo", database "judo", pool size 10).
- **PostgresqlConfiguration** (`hu.blackbelt.judo.runtime.core.guice.dao.rdbms.postgresql.PostgresqlConfiguration`): Binding annotations for PostgreSQL connection parameters (`@PostgresqlHost`, `@PostgresqlPort`, `@PostgresqlUser`, `@PostgresqlPassword`, `@PostgresqlDatabaseName`).
- **PostgresqlDataSourceProvider**: Provides a HikariCP `DataSource` for PostgreSQL connections.
- **PostgresqlMapperFactoryProvider**: Provides `MapperFactory` with PostgreSQL-specific query mappers.
- **PostgresqlRdbmsInitProvider**: Provides `RdbmsInit` for PostgreSQL schema initialization via Liquibase.
- **PostgresqlRdbmsParameterMapperProvider**: Provides `RdbmsParameterMapper` for PostgreSQL parameter binding.
- **PostgresqlRdbmsSequenceProvider**: Provides `Sequence` for PostgreSQL-native sequence management.

## Requirements

### Requirement: PostgreSQL Dialect Binding
The module SHALL bind `Dialect` to a `PostgresqlDialect` instance.

#### Scenario: PostgresqlDialect is always bound
- **GIVEN** a `JudoPostgresqlModule` is installed in the Guice injector
- **WHEN** the module's `configure()` method executes
- **THEN** `Dialect` SHALL be bound to a new `PostgresqlDialect` instance

### Requirement: Connection Configuration Binding
The module SHALL bind PostgreSQL connection parameters via qualified annotations from `PostgresqlConfiguration`.

#### Scenario: Default connection parameters are bound
- **GIVEN** a `JudoPostgresqlModule` built with default configuration
- **WHEN** `configureOptions()` is invoked
- **THEN** `@PostgresqlHost` SHALL resolve to `"localhost"`, `@PostgresqlPort` to `5432`, `@PostgresqlUser` to `"judo"`, `@PostgresqlPassword` to `"judo"`, and `@PostgresqlDatabaseName` to `"judo"`

### Requirement: DataSource Provider with Override
The module SHALL provide a PostgreSQL `DataSource` via `PostgresqlDataSourceProvider` or accept a pre-configured `DataSource` instance.

#### Scenario: Default DataSource via provider
- **GIVEN** `JudoPostgresqlModuleConfiguration` with `dataSource` set to null
- **WHEN** `configureDataSource()` is invoked
- **THEN** `DataSource` SHALL be bound via `PostgresqlDataSourceProvider` in Singleton scope

#### Scenario: Custom DataSource instance
- **GIVEN** `JudoPostgresqlModuleConfiguration` with a non-null `dataSource`
- **WHEN** `configureDataSource()` is invoked
- **THEN** `DataSource` SHALL be bound to the provided instance directly

### Requirement: RDBMS Initialization
The module SHALL bind `RdbmsInit` for PostgreSQL schema setup via Liquibase.

#### Scenario: Default RdbmsInit via provider
- **GIVEN** `JudoPostgresqlModuleConfiguration` with `rdbmsInit` set to null
- **WHEN** `configureRdbmsInit()` is invoked
- **THEN** `RdbmsInit` SHALL be bound via `PostgresqlRdbmsInitProvider` in Singleton scope

### Requirement: Sequence Provider
The module SHALL bind `Sequence` for PostgreSQL-native sequence generation.

#### Scenario: Default Sequence via provider
- **GIVEN** `JudoPostgresqlModuleConfiguration` with `sequence` set to null
- **WHEN** `configureSequence()` is invoked
- **THEN** `Sequence` SHALL be bound via `PostgresqlRdbmsSequenceProvider` in Singleton scope

### Requirement: Query Mapper Factory
The module SHALL bind `MapperFactory` with PostgreSQL-specific SQL query mappers.

#### Scenario: Default MapperFactory via provider
- **GIVEN** `JudoPostgresqlModuleConfiguration` with `mapperFactory` set to null
- **WHEN** `configureMapperFactory()` is invoked
- **THEN** `MapperFactory` SHALL be bound via `PostgresqlMapperFactoryProvider` in Singleton scope

### Requirement: PlatformTransactionManager Binding
The module SHALL bind `PlatformTransactionManager` via provider or accept a pre-configured instance.

#### Scenario: Default PlatformTransactionManager via provider
- **GIVEN** `JudoPostgresqlModuleConfiguration` with `platformTransactionManager` set to null
- **WHEN** `configurePlatformTransactionManager()` is invoked
- **THEN** `PlatformTransactionManager` SHALL be bound via `PlatformTransactionManagerProvider` in Singleton scope

### Requirement: Default Configuration Values
The module SHALL provide sensible default values for PostgreSQL configuration.

#### Scenario: Default configuration
- **GIVEN** a `JudoPostgresqlModuleConfiguration` built with no overrides
- **WHEN** default values are inspected
- **THEN** `host` SHALL be `"localhost"`, `port` SHALL be `5432`, `user` SHALL be `"judo"`, `password` SHALL be `"judo"`, `databaseName` SHALL be `"judo"`, and `poolSize` SHALL be `10`
