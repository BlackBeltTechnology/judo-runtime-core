# DAO RDBMS PostgreSQL Dialect Specification

## Purpose
Provides the PostgreSQL-specific dialect implementation for the JUDO Runtime Core DAO RDBMS layer, enabling production-grade relational database support with PostgreSQL-native SQL syntax, type mappings, sequence operations, and connection testing.

## Architecture

### Key Classes and Relationships

- **`PostgresqlDialect`** -- Implements the `Dialect` interface for PostgreSQL. Returns `"postgresql"` as the dialect name and `null` as the dual table (PostgreSQL supports `SELECT` without `FROM`).
- **`PostgresqlRdbmsInit`** -- Implements the `RdbmsInit` interface. Runs a PostgreSQL-specific initialization changelog (`liquibase/postgresql-init-changelog.xml`) before applying the main `LiquibaseModel` changelog, ensuring PostgreSQL prerequisites (e.g., extensions, custom types) are in place.
- **`PostgresqlRdbmsSequence`** -- Implements the `Sequence<Long>` interface for PostgreSQL. Provides `getNextValue()` and `getCurrentValue()` operations using PostgreSQL-native `SELECT NEXTVAL(...)` / `SELECT CURRVAL(...)` syntax. Supports auto-creation of sequences with configurable start and increment values.
- **`PostgresqlRdbmsParameterMapper`** -- Extends `DefaultRdbmsParameterMapper` with PostgreSQL-specific type mappings: maps `java.lang.String` to `TEXT` and `java.lang.Double` to `DOUBLE PRECISION`.
- **`PostgresqlMapperFactory`** -- Extends `DefaultMapperFactory` to replace the default `Function` mapper with `PostgresqlFunctionMapper`, providing PostgreSQL-specific SQL function generation.
- **`PostgresqlFunctionMapper`** -- PostgreSQL-specific implementation of `RdbmsMapper<Function>` that generates PostgreSQL-compatible SQL for aggregate and scalar functions.
- **`PostgresqlConnectionTester`** -- Utility class for testing PostgreSQL connectivity before application startup, using the PostgreSQL JDBC `Driver` directly.

## Requirements

### Requirement: PostgreSQL Dialect Identification
The `PostgresqlDialect` SHALL correctly identify itself as the PostgreSQL dialect and indicate that no dual table is needed.

#### Scenario: Return dialect name
- **GIVEN** an instance of `PostgresqlDialect`
- **WHEN** `getName()` is called
- **THEN** the method SHALL return `"postgresql"`

#### Scenario: Return null for dual table
- **GIVEN** an instance of `PostgresqlDialect`
- **WHEN** `getDualTable()` is called
- **THEN** the method SHALL return `null`, indicating PostgreSQL supports `SELECT` expressions without a `FROM` clause

### Requirement: PostgreSQL Database Initialization
The `PostgresqlRdbmsInit` SHALL initialize the PostgreSQL database with a two-phase approach: first applying PostgreSQL-specific prerequisites, then the main schema.

#### Scenario: Execute two-phase initialization
- **GIVEN** a valid `DataSource` pointing to a PostgreSQL instance, a `SimpleLiquibaseExecutor`, and a `LiquibaseModel`
- **WHEN** `PostgresqlRdbmsInit.execute(DataSource)` is called
- **THEN** the system SHALL first call `SimpleLiquibaseExecutor.executeInitiLiquibase()` with the classpath resource `"liquibase/postgresql-init-changelog.xml"`, and then call `SimpleLiquibaseExecutor.createDatabase()` with the data source and liquibase model

#### Scenario: Apply PostgreSQL prerequisites before schema
- **GIVEN** a PostgreSQL database that requires extensions or custom types
- **WHEN** `PostgresqlRdbmsInit.execute(DataSource)` is called
- **THEN** the PostgreSQL-specific init changelog SHALL be applied before the main model changelog to ensure all prerequisites are available

#### Scenario: Fail on missing dependencies
- **GIVEN** a null `SimpleLiquibaseExecutor` or null `LiquibaseModel`
- **WHEN** `PostgresqlRdbmsInit` is constructed
- **THEN** the builder SHALL throw a `NullPointerException` due to the `@NonNull` constraint

### Requirement: PostgreSQL Sequence Operations
The `PostgresqlRdbmsSequence` SHALL provide database sequence operations using PostgreSQL-native SQL syntax.

#### Scenario: Get next sequence value
- **GIVEN** a sequence name and a PostgreSQL data source
- **WHEN** `PostgresqlRdbmsSequence.getNextValue(String)` is called
- **THEN** the system SHALL execute `SELECT NEXTVAL('"<sequenceName>"')` and return the resulting `Long` value

#### Scenario: Get current sequence value
- **GIVEN** a sequence name and a PostgreSQL data source
- **WHEN** `PostgresqlRdbmsSequence.getCurrentValue(String)` is called
- **THEN** the system SHALL execute `SELECT CURRVAL('"<sequenceName>"')` and return the resulting `Long` value

#### Scenario: Auto-create sequence if not exists
- **GIVEN** `createIfNotExists` is true (the default) and the sequence does not yet exist
- **WHEN** any sequence operation is called
- **THEN** the system SHALL first execute `CREATE SEQUENCE IF NOT EXISTS "<sequenceName>"` with the configured start and increment values before querying the value

#### Scenario: Sanitize sequence name
- **GIVEN** a sequence name containing special characters (e.g., dots, hyphens)
- **WHEN** any sequence operation is called
- **THEN** the sequence name SHALL be sanitized by replacing all non-alphanumeric non-underscore characters with underscores

#### Scenario: Default start and increment
- **GIVEN** no explicit start or increment values are provided
- **WHEN** `PostgresqlRdbmsSequence` is constructed
- **THEN** the start SHALL default to `Sequence.DEFAULT_START` and increment SHALL default to `Sequence.DEFAULT_INCREMENT`

### Requirement: PostgreSQL Connection Testing
The `PostgresqlConnectionTester` SHALL verify that a PostgreSQL database is reachable before the application proceeds.

#### Scenario: Successful connection test
- **GIVEN** valid PostgreSQL URL, username, and password
- **WHEN** `PostgresqlConnectionTester.testConnection(String, String, String)` is called
- **THEN** the method SHALL establish a JDBC connection using `DriverManager.getConnection()`, close it, and return normally

#### Scenario: Failed connection test
- **GIVEN** an invalid or unreachable PostgreSQL URL
- **WHEN** `PostgresqlConnectionTester.testConnection(String, String, String)` is called
- **THEN** the method SHALL throw a `ConnectException` with a message containing `"Could not connect to postgresql"` and the underlying error details

### Requirement: PostgreSQL Parameter Type Mapping
The `PostgresqlRdbmsParameterMapper` SHALL provide PostgreSQL-specific SQL type mappings for parameter binding.

#### Scenario: Map String type to TEXT
- **GIVEN** a parameter of type `java.lang.String`
- **WHEN** the parameter mapper resolves the SQL type name
- **THEN** the type SHALL be mapped to `"TEXT"` for PostgreSQL native text storage

#### Scenario: Map Double type to DOUBLE PRECISION
- **GIVEN** a parameter of type `java.lang.Double`
- **WHEN** the parameter mapper resolves the SQL type name
- **THEN** the type SHALL be mapped to `"DOUBLE PRECISION"` for PostgreSQL numeric precision

### Requirement: PostgreSQL Function Mapping
The `PostgresqlMapperFactory` SHALL provide a PostgreSQL-specific function mapper for SQL function generation.

#### Scenario: Override default function mapper
- **GIVEN** an `RdbmsBuilder` instance
- **WHEN** `PostgresqlMapperFactory.getMappers(RdbmsBuilder)` is called
- **THEN** the returned mapper map SHALL contain `PostgresqlFunctionMapper` registered for the `Function.class` key, overriding the default function mapper from `DefaultMapperFactory`

#### Scenario: Retain non-function mappers from defaults
- **GIVEN** an `RdbmsBuilder` instance
- **WHEN** `PostgresqlMapperFactory.getMappers(RdbmsBuilder)` is called
- **THEN** all non-Function mappers from `DefaultMapperFactory` (AttributeMapper, ConstantMapper, SubSelectMapper, etc.) SHALL be preserved in the returned map
