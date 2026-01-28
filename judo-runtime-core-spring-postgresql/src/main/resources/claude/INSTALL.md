# JUDO Spring PostgreSQL Integration

## Overview

This module provides Spring Boot autoconfiguration for PostgreSQL database support in JUDO applications. It automatically configures PostgreSQL-specific components when the datasource URL contains `postgresql`.

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-spring-postgresql
- **Package**: hu.blackbelt.judo.runtime.core.spring.postgresql

## Autoconfiguration

The `JudoPostgresqlSpringConfiguration` class is activated when:
```
spring.datasource.url contains 'postgresql'
```

### Beans Provided

| Bean | Type | Description |
|------|------|-------------|
| `PostgresqlDialect` | `Dialect` | PostgreSQL SQL dialect implementation |
| `PostgresqlRdbmsSequence` | `Sequence` | Database sequence provider for ID generation |
| `PostgresqlRdbmsParameterMapper` | `RdbmsParameterMapper` | Query parameter mapping for PostgreSQL |
| `PostgresqlMapperFactory` | `MapperFactory` | PostgreSQL-specific type mappers |

## Dependencies

This module requires:
- `judo-runtime-core-spring` - Base Spring autoconfiguration
- `judo-runtime-core-dao-rdbms-postgresql` - PostgreSQL DAO implementation
- Spring Boot with configured `DataSource` and `PlatformTransactionManager`

## Usage

### Maven Dependency

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-spring-postgresql</artifactId>
    <version>${project.version}</version>
</dependency>
```

### Application Properties

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/mydb
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.username=postgres
spring.datasource.password=secret
```

## Sequence Configuration

The sequence provider is configured with defaults:
- **start**: 1
- **increment**: 1
- **createIfNotExists**: true

## Use Cases

- Production deployments
- Enterprise applications
- High-performance database requirements
- Full ACID compliance scenarios
