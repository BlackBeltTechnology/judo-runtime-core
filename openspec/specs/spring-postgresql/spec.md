# Spring PostgreSQL Autoconfiguration Specification

## Purpose
Provides Spring Boot autoconfiguration for PostgreSQL as the production database backend in JUDO Runtime Core applications, activated conditionally when the Spring datasource URL contains "postgresql".

## Architecture
- **JudoPostgresqlSpringConfiguration** (`hu.blackbelt.judo.runtime.core.spring.postgresql.JudoPostgresqlSpringConfiguration`): `@Configuration` class annotated with `@ConditionalOnExpression("'${spring.datasource.url}'.contains('postgresql')")` that provides PostgreSQL-specific beans for dialect, sequence, parameter mapper, and mapper factory.

### Bean Definitions
| Bean Method | Returns | Description |
|-------------|---------|-------------|
| `getPostgresqlDialect()` | `PostgresqlDialect` | PostgreSQL dialect instance |
| `getPostgresqlSequence()` | `Sequence` (via `PostgresqlRdbmsSequence`) | PostgreSQL-native sequence provider with configurable start, increment, and createIfNotExists |
| `getPostgresqlRdbmsParameterMapper()` | `RdbmsParameterMapper` (via `PostgresqlRdbmsParameterMapper`) | PostgreSQL-specific parameter mapper using Coercer, RdbmsModel, and IdentifierProvider |
| `getPostgresqlMapperFactory()` | `MapperFactory` (via `PostgresqlMapperFactory`) | PostgreSQL-specific SQL query mapper factory |

## Requirements

### Requirement: Conditional Activation
The configuration SHALL only be active when the Spring datasource URL contains "postgresql".

#### Scenario: Configuration activates for PostgreSQL URL
- **GIVEN** `spring.datasource.url` set to `jdbc:postgresql://localhost:5432/judo`
- **WHEN** the Spring application context initializes
- **THEN** `JudoPostgresqlSpringConfiguration` SHALL be active and all PostgreSQL beans SHALL be created

#### Scenario: Configuration does not activate for HSQLDB URL
- **GIVEN** `spring.datasource.url` set to `jdbc:hsqldb:mem:testdb`
- **WHEN** the Spring application context initializes
- **THEN** `JudoPostgresqlSpringConfiguration` SHALL NOT be active

### Requirement: PostgreSQL Dialect Bean
The configuration SHALL provide a `PostgresqlDialect` bean.

#### Scenario: PostgresqlDialect is available
- **GIVEN** the configuration is active
- **WHEN** the application context initializes
- **THEN** a `PostgresqlDialect` bean SHALL be available as the `Dialect` implementation

### Requirement: PostgreSQL Sequence Bean
The configuration SHALL provide a `Sequence` bean backed by `PostgresqlRdbmsSequence` with default parameters.

#### Scenario: Sequence is created with defaults
- **GIVEN** the configuration is active
- **WHEN** `getPostgresqlSequence()` is invoked
- **THEN** a `PostgresqlRdbmsSequence` SHALL be created with `start=1L`, `increment=1L`, `createIfNotExists=true`, using the autowired `DataSource`

### Requirement: PostgreSQL Parameter Mapper Bean
The configuration SHALL provide an `RdbmsParameterMapper` bean backed by `PostgresqlRdbmsParameterMapper`.

#### Scenario: Parameter mapper is created
- **GIVEN** the configuration is active and `IdentifierProvider`, `Dialect`, `Coercer`, and `RdbmsModel` beans are available
- **WHEN** `getPostgresqlRdbmsParameterMapper()` is invoked
- **THEN** a `PostgresqlRdbmsParameterMapper` SHALL be created with the autowired `Coercer`, `RdbmsModel`, and `IdentifierProvider`

### Requirement: PostgreSQL Mapper Factory Bean
The configuration SHALL provide a `MapperFactory` bean backed by `PostgresqlMapperFactory`.

#### Scenario: Mapper factory is available
- **GIVEN** the configuration is active
- **WHEN** the application context initializes
- **THEN** a `PostgresqlMapperFactory` bean SHALL be available as the `MapperFactory` implementation
