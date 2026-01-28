# JUDO Guice PostgreSQL Module

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-guice-postgresql
- **Version**: ${project.version}

## Overview

This module provides Google Guice integration for PostgreSQL database connectivity with HikariCP connection pooling, suitable for production environments.

## Main Components

### JudoPostgresqlModule

The primary Guice module that configures PostgreSQL database connectivity.

```java
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.postgresql.JudoPostgresqlModule;

// Basic usage with defaults
Injector injector = Guice.createInjector(
    JudoPostgresqlModule.builder().build()
);

// Production configuration
Injector injector = Guice.createInjector(
    JudoPostgresqlModule.builder()
        .host("db.example.com")
        .port(5432)
        .databaseName("myapp")
        .user("appuser")
        .password("secret")
        .poolSize(20)
        .build()
);
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | String | `"localhost"` | PostgreSQL server hostname |
| `port` | Integer | `5432` | PostgreSQL server port |
| `databaseName` | String | `"judo"` | Database name |
| `user` | String | `"judo"` | Database username |
| `password` | String | `"judo"` | Database password |
| `poolSize` | Integer | `10` | HikariCP connection pool size |
| `dataSource` | DataSource | auto | Custom DataSource (optional) |
| `platformTransactionManager` | PlatformTransactionManager | auto | Custom transaction manager |

### Provided Bindings

The module binds the following types:

| Type | Implementation | Scope |
|------|----------------|-------|
| `Dialect` | `PostgresqlDialect` | Instance |
| `DataSource` | HikariCP DataSource | Singleton |
| `MapperFactory` | PostgreSQL-specific mapper | Singleton |
| `RdbmsParameterMapper` | PostgreSQL parameter mapper | Singleton |
| `Sequence` | RDBMS sequence provider | Singleton |
| `RdbmsInit` | Database initializer | Singleton |
| `PlatformTransactionManager` | Spring transaction manager | Singleton |

## Maven Dependency

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-guice-postgresql</artifactId>
    <version>${project.version}</version>
</dependency>
```

## Usage with JudoDefaultModule

Typically used alongside the core Guice module:

```java
Injector injector = Guice.createInjector(
    JudoDefaultModule.builder()
        .asmModel(asmModel)
        .rdbmsModel(rdbmsModel)
        .build(),
    JudoPostgresqlModule.builder()
        .host("localhost")
        .port(5432)
        .databaseName("production_db")
        .user("app_user")
        .password(System.getenv("DB_PASSWORD"))
        .poolSize(25)
        .build()
);
```

## TestContainers Integration

For integration testing with real PostgreSQL:

```java
@Container
static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15");

@BeforeAll
static void setup() {
    injector = Guice.createInjector(
        JudoPostgresqlModule.builder()
            .host(postgres.getHost())
            .port(postgres.getFirstMappedPort())
            .databaseName(postgres.getDatabaseName())
            .user(postgres.getUsername())
            .password(postgres.getPassword())
            .build()
    );
}
```

## Configuration Qualifiers

Inject configuration values using qualifier annotations:

```java
@Inject
@PostgresqlConfiguration.PostgresqlHost
private String host;

@Inject
@PostgresqlConfiguration.PostgresqlPort
private Integer port;
```
