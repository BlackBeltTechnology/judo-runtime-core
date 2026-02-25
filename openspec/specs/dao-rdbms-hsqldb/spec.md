# DAO RDBMS HSQLDB Dialect Specification

## Purpose
Provides the HSQLDB-specific dialect implementation for the JUDO Runtime Core DAO RDBMS layer, enabling in-memory and embedded database support suitable for development, testing, and lightweight deployments.

## Architecture

### Key Classes and Relationships

- **`HsqldbDialect`** -- Implements the `Dialect` interface for HSQLDB. Returns `"hsqldb"` as the dialect name and `"INFORMATION_SCHEMA"."SYSTEM_USERS"` as the dual table (used for SELECT expressions that do not require a FROM clause in HSQLDB).
- **`HsqldbRdbmsInit`** -- Implements the `RdbmsInit` interface. Uses `SimpleLiquibaseExecutor` to apply the `LiquibaseModel` changelog against an HSQLDB `DataSource`, creating the database schema.
- **`HsqldbRdbmsSequence`** -- Implements the `Sequence<Long>` interface for HSQLDB. Provides `getNextValue()` and `getCurrentValue()` operations using HSQLDB-specific `CALL NEXT VALUE FOR` and `CALL CURRENT VALUE FOR` syntax. Supports auto-creation of sequences with configurable start and increment values.
- **`HsqldbRdbmsParameterMapper`** -- Extends `DefaultRdbmsParameterMapper` with HSQLDB-specific type mappings: maps `java.sql.Time` to `TIMESTAMP` and `java.lang.String` to `LONGVARCHAR`.
- **`HsqldbMapperFactory`** -- Extends `DefaultMapperFactory` to replace the default `Function` mapper with `HsqldbFunctionMapper`, providing HSQLDB-specific SQL function generation.
- **`HsqldbFunctionMapper`** -- HSQLDB-specific implementation of `RdbmsMapper<Function>` that generates HSQLDB-compatible SQL for aggregate and scalar functions.

## Requirements

### Requirement: HSQLDB Dialect Identification
The `HsqldbDialect` SHALL correctly identify itself as the HSQLDB dialect and provide the appropriate dual table for scalar queries.

#### Scenario: Return dialect name
- **GIVEN** an instance of `HsqldbDialect`
- **WHEN** `getName()` is called
- **THEN** the method SHALL return `"hsqldb"`

#### Scenario: Return dual table for scalar SELECT
- **GIVEN** an instance of `HsqldbDialect`
- **WHEN** `getDualTable()` is called
- **THEN** the method SHALL return `"INFORMATION_SCHEMA"."SYSTEM_USERS"` to enable scalar SELECT expressions without a real table

### Requirement: HSQLDB Database Initialization
The `HsqldbRdbmsInit` SHALL initialize the HSQLDB database schema by applying the Liquibase changelog model.

#### Scenario: Execute schema creation
- **GIVEN** a valid `DataSource` pointing to an HSQLDB instance, a `SimpleLiquibaseExecutor`, and a `LiquibaseModel`
- **WHEN** `HsqldbRdbmsInit.execute(DataSource)` is called
- **THEN** `SimpleLiquibaseExecutor.createDatabase()` SHALL be invoked with the data source and liquibase model, applying all changesets to create the schema

#### Scenario: Fail on missing dependencies
- **GIVEN** a null `SimpleLiquibaseExecutor` or null `LiquibaseModel`
- **WHEN** `HsqldbRdbmsInit` is constructed
- **THEN** the builder SHALL throw a `NullPointerException` due to the `@NonNull` constraint

### Requirement: HSQLDB Sequence Operations
The `HsqldbRdbmsSequence` SHALL provide database sequence operations using HSQLDB-specific SQL syntax.

#### Scenario: Get next sequence value
- **GIVEN** a sequence name and an HSQLDB data source
- **WHEN** `HsqldbRdbmsSequence.getNextValue(String)` is called
- **THEN** the system SHALL execute `CALL NEXT VALUE FOR "<sequenceName>"` and return the resulting `Long` value

#### Scenario: Get current sequence value
- **GIVEN** a sequence name and an HSQLDB data source
- **WHEN** `HsqldbRdbmsSequence.getCurrentValue(String)` is called
- **THEN** the system SHALL execute `CALL CURRENT VALUE FOR "<sequenceName>"` and return the resulting `Long` value

#### Scenario: Auto-create sequence if not exists
- **GIVEN** `createIfNotExists` is true (the default) and the sequence does not yet exist
- **WHEN** any sequence operation is called
- **THEN** the system SHALL first execute `CREATE SEQUENCE IF NOT EXISTS "<sequenceName>"` with the configured start and increment values before retrieving the value

#### Scenario: Sanitize sequence name
- **GIVEN** a sequence name containing special characters (e.g., dots, hyphens)
- **WHEN** any sequence operation is called
- **THEN** the sequence name SHALL be sanitized by replacing all non-alphanumeric non-underscore characters with underscores

#### Scenario: Default start and increment
- **GIVEN** no explicit start or increment values are provided
- **WHEN** `HsqldbRdbmsSequence` is constructed
- **THEN** the start SHALL default to `Sequence.DEFAULT_START` and increment SHALL default to `Sequence.DEFAULT_INCREMENT`

### Requirement: HSQLDB Parameter Type Mapping
The `HsqldbRdbmsParameterMapper` SHALL provide HSQLDB-specific SQL type mappings for parameter binding.

#### Scenario: Map Time type to TIMESTAMP
- **GIVEN** a parameter of type `java.sql.Time`
- **WHEN** the parameter mapper resolves the SQL type name
- **THEN** the type SHALL be mapped to `"TIMESTAMP"` (HSQLDB-specific handling)

#### Scenario: Map String type to LONGVARCHAR
- **GIVEN** a parameter of type `java.lang.String`
- **WHEN** the parameter mapper resolves the SQL type name
- **THEN** the type SHALL be mapped to `"LONGVARCHAR"` for HSQLDB compatibility

### Requirement: HSQLDB Function Mapping
The `HsqldbMapperFactory` SHALL provide an HSQLDB-specific function mapper for SQL function generation.

#### Scenario: Override default function mapper
- **GIVEN** an `RdbmsBuilder` instance
- **WHEN** `HsqldbMapperFactory.getMappers(RdbmsBuilder)` is called
- **THEN** the returned mapper map SHALL contain `HsqldbFunctionMapper` registered for the `Function.class` key, overriding the default function mapper from `DefaultMapperFactory`

#### Scenario: Retain non-function mappers from defaults
- **GIVEN** an `RdbmsBuilder` instance
- **WHEN** `HsqldbMapperFactory.getMappers(RdbmsBuilder)` is called
- **THEN** all non-Function mappers from `DefaultMapperFactory` (AttributeMapper, ConstantMapper, SubSelectMapper, etc.) SHALL be preserved in the returned map
