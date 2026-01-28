# JUDO Runtime Core DAO RDBMS Liquibase

Liquibase integration for the JUDO Runtime Core DAO layer. Provides database schema management and migrations.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-dao-rdbms-liquibase</artifactId>
</dependency>
```

## Key Components

### SimpleLiquibaseExecutor
Main executor for Liquibase operations. Handles schema creation and updates from JUDO Liquibase models.

```java
SimpleLiquibaseExecutor executor = new SimpleLiquibaseExecutor();

// Create database schema from LiquibaseModel
executor.createDatabase(dataSource, liquibaseModel);

// Drop all database objects
executor.dropDatabase(dataSource, liquibaseModel);

// Execute a classpath-based changelog
executor.executeInitiLiquibase(
    classLoader,
    "liquibase/my-changelog.xml",
    dataSource
);
```

### StreamResourceAccessor
Custom Liquibase `ResourceAccessor` that provides changelog XML from in-memory streams.
Used internally to execute changelogs generated from JUDO metamodels.

## Usage

This module is used by database dialect modules to execute schema operations:

```java
// Typical usage in dialect init classes
SimpleLiquibaseExecutor liquibaseExecutor = new SimpleLiquibaseExecutor();

// Execute initial setup (dialect-specific)
liquibaseExecutor.executeInitiLiquibase(
    getClass().getClassLoader(),
    "liquibase/postgresql-init-changelog.xml",
    dataSource
);

// Create schema from JUDO model
liquibaseExecutor.createDatabase(dataSource, liquibaseModel);
```

## Dependencies

Requires:
- `judo-runtime-core-dao-rdbms` - Base DAO RDBMS layer
- `liquibase-core` - Liquibase library
- `hu.blackbelt.judo.meta.liquibase.model` - JUDO Liquibase metamodel

## Integration

This module is a dependency of:
- `judo-runtime-core-dao-rdbms-hsqldb`
- `judo-runtime-core-dao-rdbms-postgresql`

It bridges the JUDO Liquibase metamodel with the Liquibase runtime to apply schema changes.
