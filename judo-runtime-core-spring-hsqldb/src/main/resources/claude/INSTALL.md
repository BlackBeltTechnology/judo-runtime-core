# JUDO Spring HSQLDB Integration

## Overview

This module provides Spring Boot autoconfiguration for HSQLDB database support in JUDO applications. It automatically configures HSQLDB-specific components when the datasource URL contains `hsqldb`.

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-spring-hsqldb
- **Package**: hu.blackbelt.judo.runtime.core.spring.hsqldb

## Autoconfiguration

The `JudoHsqldbSpringConfiguration` class is activated when:
```
spring.datasource.url contains 'hsqldb'
```

### Beans Provided

| Bean | Type | Description |
|------|------|-------------|
| `HsqldbDialect` | `Dialect` | HSQLDB SQL dialect implementation |
| `HsqldbRdbmsSequence` | `Sequence` | Database sequence provider for ID generation |
| `HsqldbRdbmsParameterMapper` | `RdbmsParameterMapper` | Query parameter mapping for HSQLDB |
| `HsqldbMapperFactory` | `MapperFactory` | HSQLDB-specific type mappers |

## Dependencies

This module requires:
- `judo-runtime-core-spring` - Base Spring autoconfiguration
- `judo-runtime-core-dao-rdbms-hsqldb` - HSQLDB DAO implementation
- Spring Boot with configured `DataSource` and `PlatformTransactionManager`

## Usage

### Maven Dependency

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-spring-hsqldb</artifactId>
    <version>${project.version}</version>
</dependency>
```

### Application Properties

```properties
spring.datasource.url=jdbc:hsqldb:mem:testdb
spring.datasource.driver-class-name=org.hsqldb.jdbc.JDBCDriver
spring.datasource.username=sa
spring.datasource.password=
```

## Sequence Configuration

The sequence provider is configured with defaults:
- **start**: 1
- **increment**: 1
- **createIfNotExists**: true

## Use Cases

- Development and testing environments
- In-memory database scenarios
- Embedded database applications
- Quick prototyping
