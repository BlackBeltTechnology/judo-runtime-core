# JUDO Guice HSQLDB Module

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-guice-hsqldb
- **Version**: ${project.version}

## Overview

This module provides Google Guice integration for HSQLDB, an in-memory/embedded database ideal for development and testing environments.

## Main Components

### JudoHsqldbModule

The primary Guice module that configures HSQLDB database connectivity.

```java
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb.JudoHsqldbModule;

// Basic usage with defaults
Injector injector = Guice.createInjector(
    JudoHsqldbModule.builder().build()
);

// Custom configuration
Injector injector = Guice.createInjector(
    JudoHsqldbModule.builder()
        .runServer(true)
        .databaseName("myapp")
        .port(31001)
        .build()
);
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `runServer` | Boolean | `false` | Start embedded HSQLDB server |
| `databaseName` | String | `"judo"` | Database name |
| `databasePath` | File | `./judo.db` | Path for database files |
| `port` | Integer | `31001` | Server port (when running server) |
| `dataSource` | DataSource | auto | Custom DataSource (optional) |
| `platformTransactionManager` | PlatformTransactionManager | auto | Custom transaction manager |

### Provided Bindings

The module binds the following types:

| Type | Implementation | Scope |
|------|----------------|-------|
| `Dialect` | `HsqldbDialect` | Instance |
| `DataSource` | HikariCP DataSource | Singleton |
| `MapperFactory` | HSQLDB-specific mapper | Singleton |
| `RdbmsParameterMapper` | HSQLDB parameter mapper | Singleton |
| `Sequence` | RDBMS sequence provider | Singleton |
| `RdbmsInit` | Database initializer | Singleton |
| `PlatformTransactionManager` | Spring transaction manager | Singleton |
| `Server` | HSQLDB Server (if enabled) | Singleton |

## Maven Dependency

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-guice-hsqldb</artifactId>
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
    JudoHsqldbModule.builder()
        .databaseName("testdb")
        .build()
);
```

## Testing Use Case

HSQLDB is particularly useful for unit and integration tests:

```java
@BeforeEach
void setup() {
    injector = Guice.createInjector(
        JudoHsqldbModule.builder()
            .databasePath(new File(tempDir, "test.db"))
            .build()
    );
}
```
