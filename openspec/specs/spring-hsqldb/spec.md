# Spring HSQLDB Autoconfiguration Specification

## Purpose
Provides Spring Boot autoconfiguration for HSQLDB as the database backend in JUDO Runtime Core applications, activated conditionally when the Spring datasource URL contains "hsqldb".

## Architecture
- **JudoHsqldbSpringConfiguration** (`hu.blackbelt.judo.runtime.core.spring.hsqldb.JudoHsqldbSpringConfiguration`): `@Configuration` class annotated with `@ConditionalOnExpression("'${spring.datasource.url}'.contains('hsqldb')")` that provides HSQLDB-specific beans for dialect, sequence, parameter mapper, and mapper factory.

### Bean Definitions
| Bean Method | Returns | Description |
|-------------|---------|-------------|
| `getHsqlsbDialect()` | `HsqldbDialect` | HSQLDB dialect instance |
| `getHsqlsbSequence()` | `Sequence` (via `HsqldbRdbmsSequence`) | HSQLDB sequence provider with configurable start, increment, and createIfNotExists |
| `getHsqlsbRdbmsParameterMapper()` | `RdbmsParameterMapper` (via `HsqldbRdbmsParameterMapper`) | HSQLDB-specific parameter mapper using Coercer, RdbmsModel, and IdentifierProvider |
| `getHsqlsbMapperFactory()` | `MapperFactory` (via `HsqldbMapperFactory`) | HSQLDB-specific SQL query mapper factory |

## Requirements

### Requirement: Conditional Activation
The configuration SHALL only be active when the Spring datasource URL contains "hsqldb".

#### Scenario: Configuration activates for HSQLDB URL
- **GIVEN** `spring.datasource.url` set to `jdbc:hsqldb:mem:testdb`
- **WHEN** the Spring application context initializes
- **THEN** `JudoHsqldbSpringConfiguration` SHALL be active and all HSQLDB beans SHALL be created

#### Scenario: Configuration does not activate for PostgreSQL URL
- **GIVEN** `spring.datasource.url` set to `jdbc:postgresql://localhost:5432/judo`
- **WHEN** the Spring application context initializes
- **THEN** `JudoHsqldbSpringConfiguration` SHALL NOT be active

### Requirement: HSQLDB Dialect Bean
The configuration SHALL provide an `HsqldbDialect` bean.

#### Scenario: HsqldbDialect is available
- **GIVEN** the configuration is active
- **WHEN** the application context initializes
- **THEN** a `HsqldbDialect` bean SHALL be available as the `Dialect` implementation

### Requirement: HSQLDB Sequence Bean
The configuration SHALL provide a `Sequence` bean backed by `HsqldbRdbmsSequence` with default parameters.

#### Scenario: Sequence is created with defaults
- **GIVEN** the configuration is active
- **WHEN** `getHsqlsbSequence()` is invoked
- **THEN** an `HsqldbRdbmsSequence` SHALL be created with `start=1L`, `increment=1L`, `createIfNotExists=true`, using the autowired `DataSource`

### Requirement: HSQLDB Parameter Mapper Bean
The configuration SHALL provide an `RdbmsParameterMapper` bean backed by `HsqldbRdbmsParameterMapper`.

#### Scenario: Parameter mapper is created
- **GIVEN** the configuration is active and `IdentifierProvider`, `Dialect`, `Coercer`, and `RdbmsModel` beans are available
- **WHEN** `getHsqlsbRdbmsParameterMapper()` is invoked
- **THEN** an `HsqldbRdbmsParameterMapper` SHALL be created with the autowired `Coercer`, `RdbmsModel`, and `IdentifierProvider`

### Requirement: HSQLDB Mapper Factory Bean
The configuration SHALL provide a `MapperFactory` bean backed by `HsqldbMapperFactory`.

#### Scenario: Mapper factory is available
- **GIVEN** the configuration is active
- **WHEN** the application context initializes
- **THEN** an `HsqldbMapperFactory` bean SHALL be available as the `MapperFactory` implementation
