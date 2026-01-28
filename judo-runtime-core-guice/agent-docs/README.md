# JUDO Runtime Core :: Guice

Core Google Guice dependency injection module for JUDO runtime applications.

## Overview

This module provides the primary Guice-based dependency injection configuration for JUDO applications. It wires together all core runtime components including DAO, dispatcher, security, and access management through a single configurable module.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-guice</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

## Key Components

### JudoDefaultModule
Main Guice module that configures all JUDO runtime bindings. Uses builder pattern for extensive customization.

### JudoDefaultModuleConfiguration
Configuration holder for all module settings including:
- DAO options (optimistic locking, chunk size, recursion limits)
- Dispatcher settings (metrics, validation, string handling)
- Security configuration (identifier signing secrets)
- Sequence settings (start, increment values)

### JudoModelLoader
Loads and provides access to JUDO metamodels (ASM, RDBMS, Expression, Measure, Liquibase, Keycloak).

### JudoConfigurationQualifiers
Guice binding annotations for configuration values.

### Provider Classes
Located in subpackages for different concerns:
- `accessmanager/` - AccessManager and AuthenticationInterceptor providers
- `core/` - DataTypeManager, Coercer, IdentifierProvider
- `dao/rdbms/` - DAO, statement executors, query factory providers
- `dispatcher/` - Dispatcher, ActorResolver, MetricsCollector providers
- `security/` - PasswordPolicy, RealmExtractor providers

## Usage Example

```java
// Basic setup with model loader
JudoModelLoader modelLoader = JudoModelLoader.loadFromClassloader(
    "MyModel",
    MyApplication.class.getClassLoader()
);

Injector injector = Guice.createInjector(
    JudoDefaultModule.builder()
        .judoModelLoader(modelLoader)
        .rdbmsDaoOptimisticLockEnabled(true)
        .dispatcherEnableDefaultValidation(true)
        .build()
);

// Get dispatcher for operation calls
Dispatcher dispatcher = injector.getInstance(Dispatcher.class);
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `rdbmsDaoOptimisticLockEnabled` | true | Enable optimistic locking |
| `rdbmsDaoChunkSize` | 1000 | Batch processing chunk size |
| `dispatcherMetricsReturned` | true | Include metrics in responses |
| `dispatcherEnableDefaultValidation` | true | Enable payload validation |
| `dispatcherTrimString` | false | Trim string values |
| `dispatcherCaseInsensitiveLike` | false | Case-insensitive LIKE queries |

## Related Modules

- `judo-runtime-core-guice-hsqldb` - HSQLDB database configuration
- `judo-runtime-core-guice-postgresql` - PostgreSQL database configuration
- `judo-runtime-core-guice-jetty` - Jetty server integration
- `judo-runtime-core-guice-cxf` - CXF JAX-RS integration
- `judo-runtime-core-guice-keycloak` - Keycloak security integration
- `judo-runtime-core-guice-testkit` - Testing utilities
