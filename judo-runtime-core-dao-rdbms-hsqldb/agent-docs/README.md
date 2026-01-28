# JUDO Runtime Core DAO RDBMS HSQLDB

HSQLDB dialect provider for the JUDO Runtime Core DAO layer. Ideal for development, testing, and embedded database scenarios.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-dao-rdbms-hsqldb</artifactId>
</dependency>
```

## Key Components

### HsqldbDialect
Implements `Dialect` interface for HSQLDB-specific SQL generation.
- Dialect name: `hsqldb`
- Dual table: `"INFORMATION_SCHEMA"."SYSTEM_USERS"`

### HsqldbRdbmsInit
Database initialization using Liquibase. Implements `RdbmsInit` interface.

```java
HsqldbRdbmsInit init = HsqldbRdbmsInit.builder()
    .liquibaseExecutor(liquibaseExecutor)
    .liquibaseModel(liquibaseModel)
    .build();
init.execute(dataSource);
```

### HsqldbRdbmsSequence
Implements `Sequence<Long>` for database sequence operations.

```java
HsqldbRdbmsSequence sequence = HsqldbRdbmsSequence.builder()
    .dataSource(dataSource)
    .start(1L)              // Optional, default: 1
    .increment(1L)          // Optional, default: 1
    .createIfNotExists(true) // Optional, default: true
    .build();

Long next = sequence.getNextValue("my_sequence");
Long current = sequence.getCurrentValue("my_sequence");
```

### Query Mappers
- `HsqldbMapperFactory` - Creates HSQLDB-specific query mappers
- `HsqldbFunctionMapper` - Maps abstract functions to HSQLDB SQL functions
- `HsqldbRdbmsParameterMapper` - Handles HSQLDB parameter type mapping

## Dependencies

Requires:
- `judo-runtime-core-dao-rdbms` - Base DAO RDBMS layer
- `judo-runtime-core-dao-rdbms-liquibase` - Liquibase integration
- Spring JDBC (`spring-jdbc`, `spring-tx`)

## Usage Context

This module is typically used with:
- `judo-runtime-core-guice-hsqldb` for Guice-based applications
- `judo-runtime-core-spring-hsqldb` for Spring Boot applications
