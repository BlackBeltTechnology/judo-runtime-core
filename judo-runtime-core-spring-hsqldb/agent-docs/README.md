# JUDO Runtime Core Spring HSQLDB Module

Spring Boot autoconfiguration module that provides HSQLDB database support for JUDO Runtime Core applications.

## Overview

This module provides Spring Boot autoconfiguration for integrating HSQLDB as the database backend in JUDO-based applications. It automatically configures the necessary dialect, sequence provider, parameter mapper, and mapper factory beans when HSQLDB is detected in the datasource URL.

## Key Components

### JudoHsqldbSpringConfiguration

The main Spring configuration class that provides database-specific beans:

| Bean | Type | Description |
|------|------|-------------|
| `HsqldbDialect` | `Dialect` | HSQLDB-specific SQL dialect implementation |
| `HsqldbRdbmsSequence` | `Sequence` | Sequence generator for HSQLDB databases |
| `HsqldbRdbmsParameterMapper` | `RdbmsParameterMapper` | Parameter mapping for HSQLDB queries |
| `HsqldbMapperFactory` | `MapperFactory` | Factory for HSQLDB-specific type mappers |

### Conditional Activation

The configuration is automatically activated when the datasource URL contains `hsqldb`:

```java
@ConditionalOnExpression("'${spring.datasource.url}'.contains('hsqldb')")
```

## Usage

### Maven Dependency

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-spring-hsqldb</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

### Spring Boot Configuration

Configure your `application.properties` or `application.yml`:

```properties
spring.datasource.url=jdbc:hsqldb:mem:testdb
spring.datasource.driver-class-name=org.hsqldb.jdbc.JDBCDriver
spring.datasource.username=sa
spring.datasource.password=
```

### Component Scanning

Ensure Spring component scanning includes the JUDO configuration package:

```java
@SpringBootApplication
@ComponentScan(basePackages = {
    "your.application.package",
    "hu.blackbelt.judo.runtime.core.spring.hsqldb"
})
public class YourApplication {
    public static void main(String[] args) {
        SpringApplication.run(YourApplication.class, args);
    }
}
```

## Dependencies

This module depends on:

- `judo-runtime-core` - Core runtime abstractions
- `judo-runtime-core-dao-core` - DAO core interfaces
- `judo-runtime-core-dao-rdbms` - RDBMS DAO implementation
- `judo-runtime-core-dao-rdbms-hsqldb` - HSQLDB dialect provider
- `judo-runtime-core-dispatcher` - Request dispatching
- `judo-runtime-core-accessmanager` - Access control
- `spring-boot-autoconfigure` - Spring Boot autoconfiguration

## Use Cases

HSQLDB is ideal for:

- **Development environments** - Fast startup, no external database required
- **Unit testing** - In-memory database for isolated test execution
- **Integration testing** - Lightweight database for CI/CD pipelines
- **Prototyping** - Quick setup for proof-of-concept applications

## Related Modules

- `judo-runtime-core-spring` - Base Spring autoconfiguration
- `judo-runtime-core-spring-postgresql` - PostgreSQL support for production
- `judo-runtime-core-dao-rdbms-hsqldb` - Underlying HSQLDB dialect implementation
