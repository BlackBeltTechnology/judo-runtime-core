# JUDO Runtime Core Spring PostgreSQL Module

Spring Boot autoconfiguration module that provides PostgreSQL database support for JUDO Runtime Core applications.

## Overview

This module provides Spring Boot autoconfiguration for integrating PostgreSQL as the database backend in JUDO-based applications. It automatically configures the necessary dialect, sequence provider, parameter mapper, and mapper factory beans when PostgreSQL is detected in the datasource URL.

## Key Components

### JudoPostgresqlSpringConfiguration

The main Spring configuration class that provides database-specific beans:

| Bean | Type | Description |
|------|------|-------------|
| `PostgresqlDialect` | `Dialect` | PostgreSQL-specific SQL dialect implementation |
| `PostgresqlRdbmsSequence` | `Sequence` | Sequence generator for PostgreSQL databases |
| `PostgresqlRdbmsParameterMapper` | `RdbmsParameterMapper` | Parameter mapping for PostgreSQL queries |
| `PostgresqlMapperFactory` | `MapperFactory` | Factory for PostgreSQL-specific type mappers |

### Conditional Activation

The configuration is automatically activated when the datasource URL contains `postgresql`:

```java
@ConditionalOnExpression("'${spring.datasource.url}'.contains('postgresql')")
```

## Usage

### Maven Dependency

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-spring-postgresql</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

### Spring Boot Configuration

Configure your `application.properties` or `application.yml`:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/mydb
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.username=postgres
spring.datasource.password=secret
```

### Component Scanning

Ensure Spring component scanning includes the JUDO configuration package:

```java
@SpringBootApplication
@ComponentScan(basePackages = {
    "your.application.package",
    "hu.blackbelt.judo.runtime.core.spring.postgresql"
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
- `judo-runtime-core-dao-rdbms-postgresql` - PostgreSQL dialect provider
- `judo-runtime-core-dispatcher` - Request dispatching
- `judo-runtime-core-accessmanager` - Access control
- `spring-boot-autoconfigure` - Spring Boot autoconfiguration

## Use Cases

PostgreSQL is the recommended database for:

- **Production environments** - Enterprise-grade reliability and performance
- **Staging environments** - Production-like testing
- **High-availability deployments** - Supports clustering and replication
- **Complex data requirements** - Advanced data types and indexing

## PostgreSQL-Specific Features

This module leverages PostgreSQL-specific features:

- Native sequence support for ID generation
- PostgreSQL-optimized query generation
- PostgreSQL data type mappings
- Connection pooling with HikariCP

## Related Modules

- `judo-runtime-core-spring` - Base Spring autoconfiguration
- `judo-runtime-core-spring-hsqldb` - HSQLDB support for development/testing
- `judo-runtime-core-dao-rdbms-postgresql` - Underlying PostgreSQL dialect implementation
