# JUDO Runtime Core :: Spring

Spring Boot autoconfiguration for JUDO runtime applications.

## Overview

This module provides Spring Boot autoconfiguration that automatically wires JUDO runtime components as Spring beans. It enables seamless integration with Spring-based applications, supporting transaction management, data sources, and all core JUDO services.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-spring</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

## Key Components

### JudoDefaultSpringConfiguration
Main `@Configuration` class that creates beans for:
- `AccessManager` - Authorization handling
- `DAO` - Data access operations
- `Dispatcher` - Operation dispatching
- `QueryFactory` - Query building
- `RdbmsBuilder` - RDBMS query construction
- `VariableResolver` - Environment and system variable resolution
- Statement executors (Select, Modify)
- Actor and identity resolvers

### JudoModelLoaderConfiguration
Configures model loading from classpath resources.

### JudoBaseServiceConfiguration
Base service configuration for common dependencies.

### JudoModelConfiguration
Model-specific bean configuration.

### AntlrCheckConfiguration
Validates ANTLR runtime compatibility at startup.

### JudoDataSourceCondition
Conditional configuration based on datasource availability.

## Usage Example

```java
@SpringBootApplication
@Import(JudoDefaultSpringConfiguration.class)
public class MyApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }
}

@Service
public class MyService {
    @Autowired
    private Dispatcher dispatcher;
    
    @Autowired
    private DAO dao;
    
    public void performOperation() {
        // Use dispatcher for business operations
    }
}
```

## Required Beans

The following beans must be provided by your application or database-specific modules:

- `DataSource` - JDBC data source
- `PlatformTransactionManager` - Transaction management
- `AsmModel`, `RdbmsModel`, `ExpressionModel`, `MeasureModel` - JUDO models
- `Asm2RdbmsTransformationTrace` - Model transformation trace
- `Coercer`, `DataTypeManager` - Type conversion
- `IdentifierProvider` - ID generation
- `MetricsCollector` - Performance metrics
- `Context`, `Sequence` - Runtime context
- `RdbmsParameterMapper`, `Dialect` - Database dialect support

## Configuration Properties

Configure via `application.properties` or `application.yml`:

```yaml
judo:
  model:
    name: MyModel
    path: classpath:model/
```

## Related Modules

- `judo-runtime-core-spring-hsqldb` - HSQLDB autoconfiguration
- `judo-runtime-core-spring-postgresql` - PostgreSQL autoconfiguration
