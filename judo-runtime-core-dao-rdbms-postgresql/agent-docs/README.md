# JUDO Runtime Core DAO RDBMS PostgreSQL

PostgreSQL dialect provider for the JUDO Runtime Core DAO layer. Production-ready database support.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-dao-rdbms-postgresql</artifactId>
</dependency>
```

## Key Components

### PostgresqlDialect
Implements `Dialect` interface for PostgreSQL-specific SQL generation.
- Dialect name: `postgresql`
- Dual table: `null` (PostgreSQL supports SELECT without FROM)

### PostgresqlRdbmsInit
Database initialization using Liquibase. Implements `RdbmsInit` interface.
Executes PostgreSQL-specific initialization changelog before schema creation.

```java
PostgresqlRdbmsInit init = PostgresqlRdbmsInit.builder()
    .liquibaseExecutor(liquibaseExecutor)
    .liquibaseModel(liquibaseModel)
    .build();
init.execute(dataSource);
```

### PostgresqlRdbmsSequence
Implements `Sequence<Long>` for database sequence operations.

```java
PostgresqlRdbmsSequence sequence = PostgresqlRdbmsSequence.builder()
    .dataSource(dataSource)
    .start(1L)              // Optional, default: 1
    .increment(1L)          // Optional, default: 1
    .createIfNotExists(true) // Optional, default: true
    .build();

Long next = sequence.getNextValue("my_sequence");
Long current = sequence.getCurrentValue("my_sequence");
```

### PostgresqlConnectionTester
Utility for testing PostgreSQL connectivity.

```java
PostgresqlConnectionTester.testConnection(
    "jdbc:postgresql://localhost:5432/mydb",
    "username",
    "password"
);
```

### Query Mappers
- `PostgresqlMapperFactory` - Creates PostgreSQL-specific query mappers
- `PostgresqlFunctionMapper` - Maps abstract functions to PostgreSQL SQL functions
- `PostgresqlRdbmsParameterMapper` - Handles PostgreSQL parameter type mapping

## Dependencies

Requires:
- `judo-runtime-core-dao-rdbms` - Base DAO RDBMS layer
- `judo-runtime-core-dao-rdbms-liquibase` - Liquibase integration
- `postgresql` - PostgreSQL JDBC driver
- Spring JDBC (`spring-jdbc`, `spring-tx`)

## Usage Context

This module is typically used with:
- `judo-runtime-core-guice-postgresql` for Guice-based applications
- `judo-runtime-core-spring-postgresql` for Spring Boot applications
