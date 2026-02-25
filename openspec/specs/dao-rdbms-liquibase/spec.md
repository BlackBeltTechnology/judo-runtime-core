# DAO RDBMS Liquibase Specification

## Purpose
Provides Liquibase-based database schema management for the JUDO Runtime Core DAO RDBMS layer, enabling creation, migration, and teardown of database schemas from JUDO `LiquibaseModel` metamodel instances using in-memory changelog streaming.

## Architecture

### Key Classes and Relationships

- **`SimpleLiquibaseExecutor`** -- Central class for executing Liquibase operations. Provides three main operations:
  1. `executeInitiLiquibase(ClassLoader, String, DataSource)` -- Executes a classpath-based Liquibase changelog (e.g., database-specific initialization scripts).
  2. `createDatabase(DataSource, LiquibaseModel)` -- Applies all changesets from a `LiquibaseModel` to create or update the database schema.
  3. `dropDatabase(DataSource, LiquibaseModel)` -- Drops all database objects defined in the `LiquibaseModel`.
  All operations obtain a JDBC connection from the `DataSource`, resolve the correct `Database` implementation via Liquibase's `DatabaseFactory`, and execute the changelog.
- **`StreamResourceAccessor`** -- Custom Liquibase `AbstractResourceAccessor` implementation that serves changelog XML content from in-memory `InputStream` instances rather than from the filesystem or classpath. Uses a `Map<String, InputStream>` to resolve resource paths to their content streams. Escapes resource path names to ensure URI compatibility.

### Integration Points

- **`LiquibaseModel`** (from `judo-meta-liquibase`) -- The metamodel representing the database changelog. `SimpleLiquibaseExecutor` serializes this model to an XML byte stream using `LiquibaseModel.saveLiquibaseModel()` with `LiquibaseNamespaceFixUriHandler.fixUriOutputStream()` for correct namespace handling.
- **`RdbmsInit`** interface -- Dialect-specific init classes (`HsqldbRdbmsInit`, `PostgresqlRdbmsInit`) delegate to `SimpleLiquibaseExecutor` for schema creation.
- **Liquibase Core** -- Uses `Liquibase`, `Database`, `DatabaseFactory`, `JdbcConnection`, `ClassLoaderResourceAccessor`, `CompositeResourceAccessor`, `Contexts`, and `LabelExpression` from the Liquibase library (version 4.9.1).

## Requirements

### Requirement: Schema Creation from LiquibaseModel
The `SimpleLiquibaseExecutor` SHALL apply all changesets from a `LiquibaseModel` to create or update the database schema.

#### Scenario: Create database schema
- **GIVEN** a valid `DataSource`, and a `LiquibaseModel` with a non-empty, valid resource set
- **WHEN** `SimpleLiquibaseExecutor.createDatabase(DataSource, LiquibaseModel)` is called
- **THEN** the system SHALL serialize the `LiquibaseModel` to an in-memory XML byte stream, create a `Liquibase` instance with a `CompositeResourceAccessor` (combining `StreamResourceAccessor` for the model and `ClassLoaderResourceAccessor` for supplementary resources), and call `liquibase.update(null)` to apply all changesets

#### Scenario: Skip empty model
- **GIVEN** a `LiquibaseModel` with an empty resource set (no changesets)
- **WHEN** `SimpleLiquibaseExecutor.createDatabase(DataSource, LiquibaseModel)` is called
- **THEN** the system SHALL log a warning `"Liquibase model is empty"` and return without executing any operations

#### Scenario: Skip invalid model
- **GIVEN** a `LiquibaseModel` that fails validation (`isValid()` returns false)
- **WHEN** `SimpleLiquibaseExecutor.createDatabase(DataSource, LiquibaseModel)` is called
- **THEN** the system SHALL log a warning `"Liquibase model is invalid"` and return without executing any operations

#### Scenario: Wrap execution errors in RuntimeException
- **GIVEN** a `LiquibaseModel` and a `DataSource` where the Liquibase update fails
- **WHEN** `SimpleLiquibaseExecutor.createDatabase(DataSource, LiquibaseModel)` is called
- **THEN** the system SHALL catch the exception, log `"Execute liquibase"` at error level, and throw a `RuntimeException` wrapping the original exception

### Requirement: Schema Teardown
The `SimpleLiquibaseExecutor` SHALL support dropping all database objects defined by a `LiquibaseModel`.

#### Scenario: Drop all database objects
- **GIVEN** a valid `DataSource` and a `LiquibaseModel` with a non-empty, valid resource set
- **WHEN** `SimpleLiquibaseExecutor.dropDatabase(DataSource, LiquibaseModel)` is called
- **THEN** the system SHALL create a `Liquibase` instance from the model and call `liquibase.dropAll()` to remove all managed database objects

#### Scenario: Handle drop errors
- **GIVEN** a `LiquibaseModel` and a `DataSource` where the drop operation fails
- **WHEN** `SimpleLiquibaseExecutor.dropDatabase(DataSource, LiquibaseModel)` is called
- **THEN** the system SHALL wrap the `DatabaseException` in a `RuntimeException`

### Requirement: Classpath-Based Initialization Changelog
The `SimpleLiquibaseExecutor` SHALL support executing a classpath-based Liquibase changelog for database-specific initialization.

#### Scenario: Execute init changelog from classpath
- **GIVEN** a `ClassLoader`, a classpath resource name (e.g., `"liquibase/postgresql-init-changelog.xml"`), and a valid `DataSource`
- **WHEN** `SimpleLiquibaseExecutor.executeInitiLiquibase(ClassLoader, String, DataSource)` is called
- **THEN** the system SHALL create a `Liquibase` instance using a `ClassLoaderResourceAccessor` and the provided classloader, then call `liquibase.update(new Contexts(), new LabelExpression())` to apply the changelog

#### Scenario: Handle init errors
- **GIVEN** a classpath resource that does not exist or a database connection that fails
- **WHEN** `SimpleLiquibaseExecutor.executeInitiLiquibase()` is called
- **THEN** the system SHALL log `"Error init liquibase"` at error level and throw a `RuntimeException` wrapping the original exception

### Requirement: In-Memory Changelog Resource Access
The `StreamResourceAccessor` SHALL serve Liquibase changelog content from in-memory `InputStream` instances.

#### Scenario: Open stream by path
- **GIVEN** a `StreamResourceAccessor` initialized with a map containing `{"model.changelog.xml": <inputStream>}`
- **WHEN** `openStreams(null, "model.changelog.xml")` is called
- **THEN** the method SHALL return an `InputStreamList` containing the associated `InputStream` with a URI derived from the escaped path name

#### Scenario: Return empty for unknown path
- **GIVEN** a `StreamResourceAccessor` with known entries
- **WHEN** `openStreams(null, "unknown-path.xml")` is called
- **THEN** the method SHALL return an empty `InputStreamList`

#### Scenario: Escape resource paths
- **GIVEN** a resource path containing special characters (e.g., spaces, colons)
- **WHEN** `StreamResourceAccessor.escape(String)` is called
- **THEN** all characters that are not alphanumeric, hyphen, underscore, or dot SHALL be replaced with underscores

#### Scenario: List known resources
- **GIVEN** a `StreamResourceAccessor` initialized with multiple entries
- **WHEN** `describeLocations()` is called
- **THEN** the method SHALL return a `SortedSet<String>` containing the escaped keys of all registered streams

### Requirement: Changelog Model Serialization
The `SimpleLiquibaseExecutor` SHALL correctly serialize the `LiquibaseModel` to XML with proper namespace handling before passing it to Liquibase.

#### Scenario: Serialize model with namespace fix
- **GIVEN** a valid `LiquibaseModel`
- **WHEN** `executueOnLiquibaseModel()` prepares the changelog
- **THEN** the model SHALL be saved using `LiquibaseModel.saveLiquibaseModel()` with `validateModel` set to `false` and the output stream wrapped by `LiquibaseNamespaceFixUriHandler.fixUriOutputStream()` to ensure correct XML namespace URIs

#### Scenario: Name changelog file
- **GIVEN** a `LiquibaseModel` with name `"myModel"`
- **WHEN** `executueOnLiquibaseModel()` prepares the changelog
- **THEN** the changelog resource SHALL be named `"myModel.changelog.xml"` and registered in the `StreamResourceAccessor` under that name

### Requirement: Database Connection Management
The `SimpleLiquibaseExecutor` SHALL properly obtain and release database connections.

#### Scenario: Obtain connection from DataSource
- **GIVEN** a valid `DataSource`
- **WHEN** any Liquibase operation is executed
- **THEN** the system SHALL obtain a `Connection` from `DataSource.getConnection()` and wrap it in a Liquibase `JdbcConnection`

#### Scenario: Close database after operation
- **GIVEN** a Liquibase operation has completed (successfully or with error during model operations)
- **WHEN** the operation finishes
- **THEN** the system SHALL call `database.close()` to release the connection back to the pool
