# JUDO Test Kit - Cookbook

> **📘 Setting up tests in your project?** See [TEST-CONFIGURATION.md](TEST-CONFIGURATION.md) for a complete guide on configuring dependencies, build order, and troubleshooting common issues.

## Overview

The JUDO Test Kit provides utilities for testing custom implementations (interceptors, operations, etc.) outside of an OSGi container environment. The core component is the `ReferenceInjector` utility class that automatically injects dependencies into your custom implementations.

## Problem Statement

In production, custom interceptors and operations use OSGi's `@Reference` annotation to inject dependencies:

```java
@Component(property = { "judo.model.name=example" })
public class UserCreateInterceptor implements OperationCallInterceptor {

    @Reference
    UserDao userDao;

    @Reference
    RecalculatePermissions recalculatePermissions;

    // Business logic here
}
```

However, OSGi's `@Reference` annotation has `@Retention(CLASS)`, not `@Retention(RUNTIME)`. This means:
- The annotation is processed at build time by OSGi tooling
- The annotation is **not available at runtime** via reflection
- Testing these components outside OSGi requires manual dependency injection

## Solution: ReferenceInjector

The `ReferenceInjector` utility class provides automatic dependency injection for testing by:
1. Finding all non-static, non-final fields in the target object
2. Attempting to inject instances from the Guice injector for each field type
3. Also supporting setter-based injection for methods named `set*`

## Usage

### Option 1: Using @JudoTest Annotation (Simplest & Recommended)

The `@JudoTest` annotation is part of the fixture extensions and provides the simplest way to test JUDO custom implementations. It offers flexible transaction handling with four different modes.

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest.TransactionHandling;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.testkit.util.ReferenceInjector;

class MyInterceptorTest {

    @JudoTest  // Default: AUTO_ROLLBACK
    void testInterceptor(JudoRuntimeFixture fixture) {
        // One line to create and inject all dependencies
        UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            UserCreateInterceptor.class,
            fixture.getInjector()
        );

        // Prepare test data
        Payload inputPayload = Payload.map(
            "email", "quicktest@example.com",
            "name", "Quick Test User"
        );
        
        CreateInstanceCall.CreateInstanceCallPayload payload = 
            new CreateInstanceCall.CreateInstanceCallPayload(inputPayload);
        Payload returnPayload = Payload.map("__identifier", 1L);
        
        // Execute interceptor logic
        Object result = interceptor.postCall(null, payload, returnPayload);
        
        // Verify business logic
        assertNotNull(result);
        Payload resultPayload = (Payload) result;
        assertEquals("quicktest@example.com", resultPayload.get("email"));
        
        // Transaction is automatically started and rolled back - no DB pollution!
    }

    @JudoTest(transaction = TransactionHandling.AUTO_COMMIT)
    void testWithCommit(JudoRuntimeFixture fixture) {
        // Transaction automatically committed, tables truncated
        UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            UserCreateInterceptor.class,
            fixture.getInjector()
        );

        // Create user that will be persisted
        Payload inputPayload = Payload.map(
            "email", "persistent@example.com",
            "name", "Persistent User",
            "accountBalance", 1000.0
        );
        
        CreateInstanceCall.CreateInstanceCallPayload payload = 
            new CreateInstanceCall.CreateInstanceCallPayload(inputPayload);
        Payload returnPayload = Payload.map("__identifier", 100L);
        
        // Execute and commit
        Object result = interceptor.postCall(null, payload, returnPayload);
        assertNotNull(result);
        
        // Data is committed to DB, then tables are truncated after test
        // Perfect for verifying persistence logic without affecting other tests
    }

    @JudoTest(transaction = TransactionHandling.MANUAL)
    void testManualTransaction(JudoRuntimeFixture fixture) {
        // Complete manual control
        fixture.beginTransaction();
        try {
            // Your test logic
            fixture.commitTransaction();
        } catch (Exception e) {
            fixture.rollbackTransaction();
            throw e;
        }
    }

    @JudoTest(transaction = TransactionHandling.NONE)
    void testReadOnly(JudoRuntimeFixture fixture) {
        // No transaction support - good for read-only tests
        // Fastest option
    }
}
```

**@JudoTest Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `modelName` | String | "example" | Model to use for the test |
| `dialect` | String | "hsqldb" | Database dialect ("hsqldb" or "postgresql") |
| `transaction` | TransactionHandling | AUTO_ROLLBACK | Transaction handling strategy |
| `truncateTables` | boolean | true | Truncate tables after test (AUTO_COMMIT/MANUAL) |
| `modelSource` | ModelSource | AUTO | Model loading strategy (AUTO/FILESYSTEM/CLASSPATH) |
| `modules` | Class<? extends Module>[] | {} | Custom Guice modules to install |
| `dataSourceMode` | DataSourceMode | BY_METHOD | Datasource lifecycle (BY_METHOD/BY_CLASS/SINGLETON) |
| `container` | String | "none" | Database container ("none", "postgresql", "yugabytedb") |

**@JudoTest Usage Modes:**

`@JudoTest` can be used at **method level** or **class level**:

- **Method-level**: Each test method can have different configuration
- **Class-level**: All test methods in the class share the same configuration

```java
// Class-level: All methods use same configuration
@JudoTest(modelName = "mymodel", dataSourceMode = DataSourceMode.BY_CLASS)
class MyTestClass {
    @Test void test1(JudoRuntimeFixture fixture) { }
    @Test void test2(JudoRuntimeFixture fixture) { }
}

// Method-level: Each method can be different
class MyTestClass {
    @JudoTest(modelName = "model1")
    void test1(JudoRuntimeFixture fixture) { }
    
    @JudoTest(modelName = "model2", dialect = "postgresql")
    void test2(JudoRuntimeFixture fixture) { }
}
```

**Transaction Handling Modes:**

| Mode | Behavior | Use Case |
|------|----------|----------|
| **AUTO_ROLLBACK** | Transaction started & rolled back automatically | Default - fastest, clean DB between tests |
| **AUTO_COMMIT** | Transaction started & committed automatically, tables truncated | Verify persisted data, integration tests |
| **MANUAL** | You control transaction lifecycle | Complex scenarios, savepoints |
| **NONE** | No transaction support | Read-only tests, maximum performance |

**Datasource Lifecycle Modes:**

| Mode | Scope | Isolation | Performance | Use Case |
|------|-------|-----------|-------------|----------|
| **BY_METHOD** (default) | Per test method | Maximum | Slowest | Each test needs clean DB |
| **BY_CLASS** | Per test class | Medium | Medium | Tests in class don't conflict |
| **SINGLETON** | Shared across all classes | Minimum | Fastest | Read-only or well-isolated tests |

```java
// BY_METHOD: New datasource for each test (default)
@JudoTest(dataSourceMode = DataSourceMode.BY_METHOD)
class Test1 {
    @Test void test1() { } // Fresh DB
    @Test void test2() { } // Fresh DB
}

// BY_CLASS: Shared datasource within class
@JudoTest(dataSourceMode = DataSourceMode.BY_CLASS)
class Test2 {
    @Test void test1() { } // Shared DB
    @Test void test2() { } // Same DB as test1
}

// SINGLETON: Shared across ALL test classes
@JudoTest(dataSourceMode = DataSourceMode.SINGLETON)
class Test3 {
    @Test void test1() { } // Global DB
}
@JudoTest(dataSourceMode = DataSourceMode.SINGLETON)
class Test4 {
    @Test void test1() { } // Same DB as Test3.test1
}
```

**Model Loading Strategies:**

| Mode | Behavior | Use Case |
|------|----------|----------|
| **AUTO** (default) | Try filesystem first, fallback to classpath | Development & packaged tests - most flexible |
| **FILESYSTEM** | Load only from `target/generated-test-sources/model` | Development environment, explicit control |
| **CLASSPATH** | Load only from `/model/` directory in JARs | Packaged tests, production JARs |

**Model Loading Examples:**

```java
// Example 1: Default - Auto-detect (filesystem → classpath fallback)
@JudoTest(modelName = "rackinspect")
void testWithAutoDetect(JudoRuntimeFixture fixture) {
    // Tries filesystem first, falls back to classpath if not found
}

// Example 2: Force filesystem loading (development)
@JudoTest(modelName = "rackinspect", modelSource = ModelSource.FILESYSTEM)
void testFromFilesystem(JudoRuntimeFixture fixture) {
    // Only loads from target/generated-test-sources/model
    // Fails if model not found on filesystem
}

// Example 3: Force classpath loading (packaged JAR tests)
@JudoTest(modelName = "rackinspect", modelSource = ModelSource.CLASSPATH)
void testFromClasspath(JudoRuntimeFixture fixture) {
    // Only loads from /model/ directory in JAR or test resources
    // Perfect for testing packaged applications
}
```

**Database Container Options:**

By default, tests use an in-memory HSQLDB database. You can configure the database using annotations or environment variables.

**Configuration Priority:**
1. **Environment Variables** (highest priority)
2. **Annotation Parameters**
3. **Smart Defaults** (lowest priority)

**Environment Variables:**
- `JUDO_TEST_DIALECT` - Database dialect ("hsqldb", "postgresql")
- `JUDO_TEST_CONTAINER` - Container type ("none", "postgresql", "yugabytedb")

**Database-Specific Environment Variables:**

Each database container has its own set of configuration environment variables that are automatically set by the testkit. Understanding these variables is important if you need to troubleshoot container startup or debug database connection issues.

**PostgreSQL Container (Testcontainers):**
- Container uses standard `POSTGRES_*` environment variables
- `POSTGRES_DB` - Database name (default: "test")
- `POSTGRES_USER` - Username (default: "test")
- `POSTGRES_PASSWORD` - Password (default: "test")
- Managed automatically by Testcontainers
- Port: Automatically allocated by Testcontainers (typically 5432 inside container)

**YugabyteDB Container:**
- Container uses `YSQL_*` environment variables (**not** `POSTGRES_*`)
- `YSQL_DB` - Database name (default: "yugabyte")
- `YSQL_USER` - Username (default: "yugabyte")
- `YSQL_PASSWORD` - Password (default: "yugabyte")
- Port: 5433 (YugabyteDB YSQL default port)
- **Important**: YugabyteDB uses Yugabyted process which ignores `POSTGRES_*` variables

**HSQLDB (In-Memory):**
- No container required
- No environment variables needed
- Fully in-memory, created per test
- Fastest option for local development

**Why Different Environment Variables?**

YugabyteDB uses a different process architecture than standard PostgreSQL:
- **PostgreSQL**: Uses the `postgres` binary and reads `POSTGRES_*` environment variables
- **YugabyteDB**: Uses the `yugabyted` binary which manages the YSQL (Yugabyte SQL) layer
- The `yugabyted` process expects `YSQL_*` environment variables to configure the YSQL API
- Using `POSTGRES_*` variables with YugabyteDB will result in the variables being ignored

**Debugging Container Issues:**

If you experience issues with database containers:

```java
// Enable container logging to see what environment variables are set
@JudoTest(container = "yugabytedb")
void debugContainerSetup(JudoRuntimeFixture fixture) {
    // The fixture provides access to datasource configuration
    // You can inspect the JDBC URL and connection properties
    System.out.println("JDBC URL: " + fixture.getJdbcUrl());
    System.out.println("Username: " + fixture.getUsername());
    
    // For detailed container logs, enable testcontainers logging:
    // - Add to logback-test.xml:
    //   <logger name="org.testcontainers" level="DEBUG"/>
}
```

**Common Mistakes:**

1. ❌ **Wrong**: Trying to configure YugabyteDB with `POSTGRES_DB` environment variable
   ```java
   // This won't work - YugabyteDB ignores POSTGRES_* variables
   container.addEnv("POSTGRES_DB", "mydb");  // Ignored!
   ```

2. ✅ **Correct**: Use `YSQL_*` variables for YugabyteDB
   ```java
   // This works - YugabyteDB reads YSQL_* variables
   container.addEnv("YSQL_DB", "mydb");  // Works!
   ```

3. ❌ **Wrong**: Using YSQL variables with PostgreSQL
   ```java
   // PostgreSQL doesn't recognize YSQL_* variables
   postgresContainer.addEnv("YSQL_DB", "mydb");  // Ignored!
   ```

4. ✅ **Correct**: Use `POSTGRES_*` variables for PostgreSQL
   ```java
   // PostgreSQL reads POSTGRES_* variables
   postgresContainer.addEnv("POSTGRES_DB", "mydb");  // Works!
   ```

**Quick Reference Table:**

| Database | Container Type | Env Variable Prefix | Database Name Var | User Var | Password Var | Default Port |
|----------|---------------|---------------------|-------------------|----------|--------------|--------------|
| **HSQLDB** | None (in-memory) | N/A | N/A | N/A | N/A | N/A |
| **PostgreSQL** | Testcontainers | `POSTGRES_*` | `POSTGRES_DB` | `POSTGRES_USER` | `POSTGRES_PASSWORD` | 5432 |
| **YugabyteDB** | Custom container | `YSQL_*` | `YSQL_DB` | `YSQL_USER` | `YSQL_PASSWORD` | 5433 |

```bash
# Run all tests with PostgreSQL container
export JUDO_TEST_DIALECT=postgresql
export JUDO_TEST_CONTAINER=postgresql
mvn test

# Run tests with YugabyteDB
export JUDO_TEST_DIALECT=postgresql
export JUDO_TEST_CONTAINER=yugabytedb
mvn test

# Override in CI/CD pipeline
JUDO_TEST_DIALECT=postgresql JUDO_TEST_CONTAINER=postgresql mvn test
```

**Annotation Configuration:**

```java
// Example 1: PostgreSQL container with Testcontainers
@JudoTest(dialect = "postgresql", container = "postgresql")
void testWithPostgresql(JudoRuntimeFixture fixture) {
    // Uses Testcontainers PostgreSQL
    // Automatic port allocation, cleanup after test
}

// Example 2: YugabyteDB container
@JudoTest(dialect = "postgresql", container = "yugabytedb")
void testWithYugabyteDB(JudoRuntimeFixture fixture) {
    // Uses custom YugabyteDB container
    // Compatible with PostgreSQL protocol
}

// Example 3: Auto-detect PostgreSQL container
@JudoTest(dialect = "postgresql")  // container auto-set to "postgresql"
void testAutoDetectContainer(JudoRuntimeFixture fixture) {
    // When dialect=postgresql and no container specified,
    // automatically uses postgresql container
}

// Example 4: In-memory HSQLDB (default, fastest)
@JudoTest  // defaults: dialect=hsqldb, container=none
void testWithHSQLDB(JudoRuntimeFixture fixture) {
    // Fast in-memory database
    // No container overhead
}
```

**Smart Defaults:**
- Default dialect: `hsqldb`
- Default container: `none` (in-memory)
- Auto-detection: When `dialect=postgresql` and `container=none`, automatically uses `container=postgresql`

**Container Lifecycle:**
- Containers are managed based on `dataSourceMode`
- `BY_METHOD`: New container per test (isolated but slower)
- `BY_CLASS`: Container shared within test class (balanced)
- `SINGLETON`: Container shared across all tests (fastest for integration suites)

**Use Cases:**

| Scenario | Configuration | Performance | Use Case |
|----------|--------------|-------------|----------|
| **Local development** | Default (HSQLDB) | Fastest | Quick feedback during development |
| **CI/CD PostgreSQL** | `JUDO_TEST_DIALECT=postgresql` | Medium | Match production database |
| **Integration tests** | `container=postgresql`<br>`dataSourceMode=SINGLETON` | Fast | Large test suites with isolation |
| **Distributed DB testing** | `container=yugabytedb` | Slower | Specific YugabyteDB features |

**Custom Guice Modules:**

You can provide custom Guice modules to add or override bindings:

```java
// Define your custom module
public class TestMockModule extends AbstractModule {
    @Override
    protected void configure() {
        // Bind mock implementations
        bind(EmailService.class).toInstance(Mockito.mock(EmailService.class));
        bind(PaymentGateway.class).to(TestPaymentGateway.class);
        
        // Override default configurations
        bind(String.class).annotatedWith(Names.named("apiUrl"))
            .toInstance("http://test-api.example.com");
    }
}

// Use custom module in test
@JudoTest(modules = { TestMockModule.class })
void testWithMocks(JudoRuntimeFixture fixture) {
    // EmailService and PaymentGateway are now mocked
    // Your interceptors will get the mock instances
}

// Multiple custom modules
@JudoTest(modules = { TestMockModule.class, CustomConfigModule.class })
void testWithMultipleModules(JudoRuntimeFixture fixture) {
    // All modules are combined
}
```

**Complete Examples:**

```java
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.CreateInstanceCall;
import hu.blackbelt.rackinspect.interceptors.user.UserCreateInterceptor;

class UserCreateInterceptorTest {
    
    // Example 1: Most common - Test business logic without persisting
    @JudoTest  // Uses AUTO_ROLLBACK by default
    void testUserCreation(JudoRuntimeFixture fixture) {
        // 1. Create and inject interceptor
        UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            UserCreateInterceptor.class,
            fixture.getInjector()
        );
        
        // 2. Prepare test data
        Payload inputPayload = Payload.map(
            "email", "test@example.com",
            "form_primaryAddressPostalCode", "1234",
            "form_primaryAddressCity", "Budapest",
            "form_primaryPhone", "+36301234567"
        );
        
        CreateInstanceCall.CreateInstanceCallPayload payload = 
            new CreateInstanceCall.CreateInstanceCallPayload(inputPayload);
        Payload returnPayload = Payload.map("__identifier", 1L);
        
        // 3. Execute and verify
        Object result = interceptor.postCall(null, payload, returnPayload);
        assertNotNull(result);
        
        // Transaction automatically rolled back - database stays clean!
    }
    
    // Example 2: Verify data persistence with cleanup
    @JudoTest(transaction = TransactionHandling.AUTO_COMMIT)
    void testDataPersistence(JudoRuntimeFixture fixture) {
        UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            UserCreateInterceptor.class,
            fixture.getInjector()
        );
        
        // Test logic that creates data
        // Changes are committed to database
        // Tables are automatically truncated after test
    }
    
    // Example 3: Complex transaction scenario with savepoints
    @JudoTest(transaction = TransactionHandling.MANUAL)
    void testWithSavepoints(JudoRuntimeFixture fixture) {
        fixture.beginTransaction();
        
        UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            UserCreateInterceptor.class,
            fixture.getInjector()
        );
        
        try {
            // Create first user successfully
            Payload user1 = Payload.map(
                "email", "user1@example.com",
                "name", "User One"
            );
            CreateInstanceCall.CreateInstanceCallPayload payload1 = 
                new CreateInstanceCall.CreateInstanceCallPayload(user1);
            Object result1 = interceptor.postCall(null, payload1, 
                Payload.map("__identifier", 1L));
            assertNotNull(result1);
            
            // Create savepoint before risky operation
            Object savepoint = fixture.createSavePoint();
            
            try {
                // Try to create user with potentially invalid data
                Payload user2 = Payload.map(
                    "email", "invalid@",  // Might fail validation
                    "name", "Invalid User"
                );
                CreateInstanceCall.CreateInstanceCallPayload payload2 = 
                    new CreateInstanceCall.CreateInstanceCallPayload(user2);
                interceptor.postCall(null, payload2, 
                    Payload.map("__identifier", 2L));
            } catch (Exception e) {
                // Rollback to savepoint, keeping user1
                fixture.rollbackToSavePoint(savepoint);
                // Continue processing...
            }
            
            // Create third user after partial rollback
            Payload user3 = Payload.map(
                "email", "user3@example.com",
                "name", "User Three"
            );
            CreateInstanceCall.CreateInstanceCallPayload payload3 = 
                new CreateInstanceCall.CreateInstanceCallPayload(user3);
            Object result3 = interceptor.postCall(null, payload3, 
                Payload.map("__identifier", 3L));
            assertNotNull(result3);
            
            fixture.commitTransaction();
            // user1 and user3 committed, user2 rolled back
        } catch (Exception e) {
            fixture.rollbackTransaction();
            throw e;
        }
    }
    
    // Example 4: Read-only test, no transaction overhead
    @JudoTest(transaction = TransactionHandling.NONE)
    void testReadOnly(JudoRuntimeFixture fixture) {
        // Query existing data without modifying
        // No transaction overhead - fastest option
        assertNotNull(fixture.getInjector());
        assertNotNull(fixture.modelHolder);
    }
}
```

### Option 2: Using JUnit Jupiter @RegisterExtension (Recommended for Production)

The testkit provides several extensions for different use cases:

#### 2.1: JudoRuntimeExtension (Full Automatic Setup)

Complete setup with datasource, runtime, and transaction management:

```java
import com.google.inject.AbstractModule;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.testkit.util.ReferenceInjector;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

class UserCreateInterceptorTest {

    @RegisterExtension
    static JudoRuntimeExtension extension = new JudoRuntimeExtension(
        "example",  // Model name
        new AbstractModule() {
            @Override
            protected void configure() {
                // Custom Guice bindings if needed
            }
        }
    );

    @Test
    void testUserCreation(JudoRuntimeFixture fixture) {
        // Everything is set up automatically:
        // - Datasource created
        // - Models loaded
        // - Transaction started
        // - Fixture injected as parameter

        UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            UserCreateInterceptor.class,
            fixture.getInjector()
        );

        // Prepare complete test data
        Payload inputPayload = Payload.map(
            "email", "newuser@example.com",
            "name", "New User",
            "form_primaryAddressPostalCode", "1234",
            "form_primaryAddressCity", "Budapest",
            "form_primaryPhone", "+36301234567"
        );
        
        CreateInstanceCall.CreateInstanceCallPayload payload = 
            new CreateInstanceCall.CreateInstanceCallPayload(inputPayload);
        Payload returnPayload = Payload.map("__identifier", 42L);
        
        // Execute interceptor
        Object result = interceptor.postCall(null, payload, returnPayload);
        
        // Verify business logic executed correctly
        assertNotNull(result);
        Payload resultPayload = (Payload) result;
        assertEquals("newuser@example.com", resultPayload.get("email"));
        assertEquals("1234", resultPayload.get("form_primaryAddressPostalCode"));
        
        // Transaction automatically committed and tables truncated after test
    }
}
```

**Features:**
- ✅ Automatic datasource setup/teardown
- ✅ JUDO runtime initialization with model loading
- ✅ Automatic transaction management (begin/commit)
- ✅ Table truncation after each test (clean state)
- ✅ JudoRuntimeFixture parameter injection

#### 2.2: JudoDatasourceByClassExtension (Per-Class Datasource)

More control over runtime initialization with datasource per test class:

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceByClassExtension;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.testkit.util.ReferenceInjector;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

class MyInterceptorTest {

    @RegisterExtension
    static JudoDatasourceByClassExtension datasourceExtension =
        new JudoDatasourceByClassExtension();

    private JudoRuntimeFixture runtimeFixture;

    @BeforeEach
    void setUp(JudoDatasourceFixture datasourceFixture) throws Exception {
        // Manually initialize runtime with provided datasource
        runtimeFixture = new JudoRuntimeFixture();
        runtimeFixture.prepare("example", datasourceFixture);
        runtimeFixture.init(new AbstractModule() {}, this);
        runtimeFixture.beginTransaction();
    }

    @AfterEach
    void tearDown() {
        if (runtimeFixture != null) {
            runtimeFixture.commitTransaction();
        }
    }

    @Test
    void testInterceptor(JudoDatasourceFixture datasourceFixture) {
        UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            UserCreateInterceptor.class,
            runtimeFixture.getInjector()
        );

        // Execute test with manual transaction control
        Payload inputPayload = Payload.map(
            "email", "manual@example.com",
            "name", "Manual Test User"
        );
        
        CreateInstanceCall.CreateInstanceCallPayload payload = 
            new CreateInstanceCall.CreateInstanceCallPayload(inputPayload);
        Payload returnPayload = Payload.map("__identifier", 10L);
        
        Object result = interceptor.postCall(null, payload, returnPayload);
        assertNotNull(result);
        
        // Transaction managed in setUp/tearDown methods
    }
}
```

**Features:**
- ✅ Datasource per test class (isolation)
- ✅ Manual runtime initialization control
- ✅ JudoDatasourceFixture parameter injection
- ⚠️ You manage transaction lifecycle

**Use when:** You need different runtime configurations per test class.

#### 2.3: JudoDatasourceSingletonExtension (Shared Datasource)

Optimized for large test suites with a shared singleton datasource:

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceSingletonExtension;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceFixture;
import org.junit.jupiter.api.extension.RegisterExtension;

class MyInterceptorTest {

    @RegisterExtension
    static JudoDatasourceSingletonExtension datasourceExtension =
        new JudoDatasourceSingletonExtension();

    // Same setup as JudoDatasourceByClassExtension
    // But datasource is shared across ALL test classes
}
```

**Features:**
- ✅ Singleton datasource shared across all test classes
- ✅ Better performance for large test suites
- ✅ Initialized once and reused
- ⚠️ Less isolation between test classes

**Use when:** You have many test classes and want to optimize performance.

#### Which Extension Should I Use?

| Extension | Use Case | Transaction Mgmt | Datasource Lifecycle | Best For |
|-----------|----------|------------------|---------------------|----------|
| **JudoRuntimeExtension** | Complete automatic setup | ✅ Automatic | Per test class | Most tests (recommended) |
| **JudoDatasourceByClassExtension** | Manual runtime control | ❌ Manual | Per test class | Custom configurations |
| **JudoDatasourceSingletonExtension** | Performance optimization | ❌ Manual | Singleton (all classes) | Large test suites |

### Option 3: Using @BeforeEach/@AfterEach (Full Manual Control)

For complete control over the fixture lifecycle:

```java
class UserCreateInterceptorTest {

    private DataSource dataSource;
    private JudoRuntimeFixture runtimeFixture;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize datasource
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:test");
        config.setUsername("SA");
        config.setPassword("");
        dataSource = new HikariDataSource(config);

        // Initialize runtime fixture
        runtimeFixture = new JudoRuntimeFixture();
        runtimeFixture.prepare("example", dataSource, DIALECT_HSQLDB);
        runtimeFixture.init(new AbstractModule() {
            @Override
            protected void configure() {
                // Custom bindings
            }
        }, null);
    }

    @AfterEach
    void tearDown() {
        if (runtimeFixture != null) {
            runtimeFixture.tearDown();
        }
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }

    @Test
    void testInterceptor() {
        // Create your interceptor instance
        UserCreateInterceptor interceptor = new UserCreateInterceptor();

        // Inject all dependencies automatically
        ReferenceInjector.injectReferences(interceptor, runtimeFixture.getInjector());

        // Now all fields (userDao, recalculatePermissions, etc.) are injected!
        
        // Prepare test payload
        Payload inputPayload = Payload.map(
            "email", "fullcontrol@example.com",
            "name", "Full Control User",
            "status", "ACTIVE"
        );
        
        CreateInstanceCall.CreateInstanceCallPayload payload = 
            new CreateInstanceCall.CreateInstanceCallPayload(inputPayload);
        Payload returnPayload = Payload.map("__identifier", 99L);
        
        // Execute business logic
        runtimeFixture.beginTransaction();
        try {
            Object result = interceptor.postCall(null, payload, returnPayload);
            assertNotNull(result);
            
            Payload resultPayload = (Payload) result;
            assertEquals("ACTIVE", resultPayload.get("status"));
            
            runtimeFixture.commitTransaction();
        } catch (Exception e) {
            runtimeFixture.rollbackTransaction();
            throw e;
        }
    }
}
```

### Advanced: Custom Guice Modules for Testing

Custom Guice modules allow you to inject mocks, test implementations, or custom configurations:

```java
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import org.mockito.Mockito;

// 1. Define a custom module with test bindings
public class EmailTestModule extends AbstractModule {
    @Override
    protected void configure() {
        // Mock external email service
        EmailService mockEmailService = Mockito.mock(EmailService.class);
        Mockito.when(mockEmailService.send(Mockito.any()))
            .thenReturn(true);
        bind(EmailService.class).toInstance(mockEmailService);
    }
}

// 2. Define another module for configuration
public class TestConfigModule extends AbstractModule {
    @Override
    protected void configure() {
        // Override configuration values
        bind(Integer.class).annotatedWith(Names.named("maxRetries"))
            .toInstance(1);  // Reduce retries for faster tests
        bind(String.class).annotatedWith(Names.named("environment"))
            .toInstance("test");
    }
}

// 3. Use modules in your test
class NotificationInterceptorTest {
    
    @JudoTest(modules = { EmailTestModule.class, TestConfigModule.class })
    void testEmailNotification(JudoRuntimeFixture fixture) {
        // Create interceptor with mocked dependencies
        NotificationInterceptor interceptor = ReferenceInjector.createAndInject(
            NotificationInterceptor.class,
            fixture.getInjector()
        );
        
        // The interceptor will use the mocked EmailService
        Payload input = Payload.map(
            "email", "test@example.com",
            "message", "Test notification"
        );
        
        Object result = interceptor.sendNotification(input);
        
        assertNotNull(result);
        // Verify mock was called
        EmailService emailService = fixture.getInjector()
            .getInstance(EmailService.class);
        Mockito.verify(emailService, Mockito.times(1))
            .send(Mockito.any());
    }
    
    @JudoTest(modules = { EmailTestModule.class })
    void testEmailFailureHandling(JudoRuntimeFixture fixture) {
        // Get the mock and configure it to fail
        EmailService mockEmail = fixture.getInjector()
            .getInstance(EmailService.class);
        Mockito.when(mockEmail.send(Mockito.any()))
            .thenReturn(false);
        
        NotificationInterceptor interceptor = ReferenceInjector.createAndInject(
            NotificationInterceptor.class,
            fixture.getInjector()
        );
        
        // Test failure handling
        Payload input = Payload.map("email", "test@example.com");
        
        assertThrows(EmailException.class, () -> {
            interceptor.sendNotification(input);
        });
    }
}
```

**Use Cases for Custom Modules:**

1. **Mock External Services**: Database connections, APIs, email services
2. **Test-Specific Implementations**: Simplified versions for faster tests
3. **Configuration Overrides**: Test-specific settings and timeouts
4. **Spy on Interactions**: Use Mockito spies to verify behavior
5. **Fixture Data Providers**: Custom data generators for tests

### Advanced: Manual Transaction Control

```java
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.CreateInstanceCall;
import static org.junit.jupiter.api.Assertions.*;

@Test
void testWithTransaction(JudoRuntimeFixture fixture) {
    // Create and inject interceptor
    UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
        UserCreateInterceptor.class,
        fixture.getInjector()
    );

    // Manual transaction control
    fixture.beginTransaction();

    try {
        // Prepare test data
        Payload inputPayload = Payload.map(
            "email", "txtest@example.com",
            "name", "Transaction Test",
            "accountBalance", 500.0,
            "status", "PENDING"
        );
        
        CreateInstanceCall.CreateInstanceCallPayload payload = 
            new CreateInstanceCall.CreateInstanceCallPayload(inputPayload);
        Payload returnPayload = Payload.map("__identifier", 200L);
        
        // Execute test logic
        Object result = interceptor.postCall(null, payload, returnPayload);

        // Verify results
        assertNotNull(result);
        Payload resultPayload = (Payload) result;
        assertEquals("PENDING", resultPayload.get("status"));
        assertEquals(500.0, resultPayload.get("accountBalance"));

        // Commit only if validation passes
        if (((Double) resultPayload.get("accountBalance")) > 0) {
            fixture.commitTransaction();
        } else {
            fixture.rollbackTransaction();
        }
    } catch (Exception e) {
        fixture.rollbackTransaction();
        throw e;
    }
}
```

### Testing with Savepoints

```java
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.CreateInstanceCall;
import static org.junit.jupiter.api.Assertions.*;

@Test
void testWithSavepoints(JudoRuntimeFixture fixture) {
    UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
        UserCreateInterceptor.class,
        fixture.getInjector()
    );

    fixture.beginTransaction();

    try {
        // Create first user successfully
        Payload user1Payload = Payload.map(
            "email", "user1@savepoint.com",
            "name", "User One",
            "accountBalance", 1000.0
        );
        CreateInstanceCall.CreateInstanceCallPayload payload1 = 
            new CreateInstanceCall.CreateInstanceCallPayload(user1Payload);
        Object result1 = interceptor.postCall(null, payload1, 
            Payload.map("__identifier", 1L));
        assertNotNull(result1);

        // Create savepoint before risky operation
        Object savepoint = fixture.createSavePoint();

        try {
            // Try to create user with negative balance (business rule violation)
            Payload user2Payload = Payload.map(
                "email", "user2@savepoint.com",
                "name", "User Two",
                "accountBalance", -500.0  // Invalid!
            );
            CreateInstanceCall.CreateInstanceCallPayload payload2 = 
                new CreateInstanceCall.CreateInstanceCallPayload(user2Payload);
            Object result2 = interceptor.postCall(null, payload2, 
                Payload.map("__identifier", 2L));
            
            // Validate business rule
            Payload result2Payload = (Payload) result2;
            if ((Double) result2Payload.get("accountBalance") < 0) {
                throw new IllegalStateException("Negative balance not allowed");
            }
        } catch (Exception e) {
            // Rollback to savepoint, keeping user1
            fixture.rollbackToSavePoint(savepoint);
            // Log or handle the error as needed
        }

        // Create third user after partial rollback
        Payload user3Payload = Payload.map(
            "email", "user3@savepoint.com",
            "name", "User Three",
            "accountBalance", 750.0
        );
        CreateInstanceCall.CreateInstanceCallPayload payload3 = 
            new CreateInstanceCall.CreateInstanceCallPayload(user3Payload);
        Object result3 = interceptor.postCall(null, payload3, 
            Payload.map("__identifier", 3L));
        assertNotNull(result3);

        // Commit: user1 and user3 persisted, user2 rolled back
        fixture.commitTransaction();
    } catch (Exception e) {
        fixture.rollbackTransaction();
        throw e;
    }
}
```

## Files

### Core Utilities

**Dependency Injection:**
- **`ReferenceInjector.java`** (`testkit/util/`)
  - Injects all non-static, non-final fields
  - Supports setter-based injection
  - Safe: skips types not available in the injector

**Fixture Extensions:**
- **`JudoRuntimeFixture.java`** (`fixture/`)
  - JUDO runtime initialization
  - Database setup, model loading, Guice injector
  - Transaction management (begin/commit/rollback/savepoints)
  - Provides `getInjector()` for dependency injection

- **`JudoDatasourceFixture.java`** (`fixture/`)
  - Database setup and management
  - Supports HSQLDB and PostgreSQL
  - Table truncation utilities

**JUnit Jupiter Extensions:**
- **`@JudoTest`** (`fixture/`) - Annotation-based testing (recommended)
- **`JudoTestExtension.java`** (`fixture/`) - Powers @JudoTest annotation
- **`JudoRuntimeExtension.java`** (`fixture/`) - Full automatic setup
- **`JudoDatasourceByClassExtension.java`** (`fixture/`) - Per-class datasource
- **`JudoDatasourceSingletonExtension.java`** (`fixture/`) - Shared singleton datasource
- **`JudoRuntimeByClassExtension.java`** (`fixture/`) - Minimal runtime setup

### Example Files

**Unit Tests:**
- **`ReferenceInjectorTest.java`** - Unit tests for dependency injection

**Integration Test Examples:**
- **`JudoTestAnnotationExamples.java`** - Complete @JudoTest examples with all transaction modes
- **`ExtensionBasedTest.java`** - Examples for all JUnit extensions
- **`AnnotationBasedTest.java`** - Legacy annotation examples (deprecated)
- **`InterceptorIntegrationTest.java`** - Real-world interceptor testing patterns

## How It Works

### Field Injection

The `ReferenceInjector` scans the class hierarchy for injectable fields:

```java
class MyInterceptor {
    UserDao userDao;           // ✓ Injected (non-static, non-final)
    PartnerDao partnerDao;     // ✓ Injected

    static Logger log;         // ✗ Skipped (static)
    final String constant;     // ✗ Skipped (final)
    int count;                 // ✗ Skipped (primitive)
}
```

### Setter Injection

Methods starting with "set" and having exactly one non-primitive parameter:

```java
class MyOperation {
    private UserDao userDao;

    public void setUserDao(UserDao userDao) {  // ✓ Injected
        this.userDao = userDao;
    }
}
```

### Safe Injection

If a field type is not bound in the Guice injector, it's simply skipped (no exception thrown). This allows you to have optional dependencies or fields that don't need injection.

## Technical Notes

### Why Not Use @Reference?

OSGi's `@Reference` annotation has `@Retention(CLASS)`:
```java
@Retention(CLASS)
@Target({METHOD, FIELD, PARAMETER})
public @interface Reference { ... }
```

This means the annotation information is:
- Available in `.class` files (for OSGi tooling)
- **Not available at runtime** (can't be read via reflection)

The `ReferenceInjector` works around this by using a convention-based approach instead of annotation-based injection.

### Guice vs OSGi

- **Production**: Uses OSGi Declarative Services for dependency injection
- **Testing**: Uses Google Guice for dependency injection (provided by JUDO runtime)

The `ReferenceInjector` bridges the gap, allowing OSGi components to be tested with Guice.

## Benefits

1. **Easy Testing**: Test custom implementations without OSGi container
2. **Automatic Injection**: No manual setup of dependencies
3. **Type-Safe**: Uses reflection and Guice's type system
4. **Convention-Based**: Works without relying on runtime annotation availability
5. **Safe**: Gracefully handles missing dependencies

## See Also

- `ReferenceInjectorTest.java` - Complete unit test examples
- `InterceptorIntegrationTest.java` - Integration test patterns
- JUDO Runtime Core documentation

## Environment Variables Mocking

The TestKit includes utilities for mocking environment variables during tests, allowing you to test code that depends on environment configuration.

### Overview

The environment variable mocking system consists of two main classes:

- **`EnvironmentVariables`** - High-level API for setting environment variables in tests
- **`EnvironmentVariableMocker`** - Low-level mocking infrastructure using Mockito

### Why Mock Environment Variables?

Many applications read configuration from environment variables:

```java
public class AppConfig {
    private String apiUrl = System.getenv("API_URL");
    private String apiKey = System.getenv("API_KEY");
    private int timeout = Integer.parseInt(System.getenv("TIMEOUT"));
}
```

Testing this code requires setting environment variables, but:
- ❌ You can't modify real environment variables in Java
- ❌ Setting OS-level variables affects all tests
- ❌ Cleanup between tests is difficult

The mocking utilities solve these problems by temporarily replacing environment variables during test execution.

### Basic Usage with EnvironmentVariables

#### Simple Example

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.util.EnvironmentVariables;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.EnvironmentVariableMocker;
import static org.junit.jupiter.api.Assertions.*;

@Test
void testWithEnvironmentVariable() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    new EnvironmentVariables("API_URL", "https://test-api.example.com")
        .execute(() -> {
            assertEquals("https://test-api.example.com", System.getenv("API_URL"));
            
            // Your test code that depends on API_URL
            AppConfig config = new AppConfig();
            assertEquals("https://test-api.example.com", config.getApiUrl());
        });
    
    // After execute(), environment is restored
    assertNull(System.getenv("API_URL"));
}
```

#### Multiple Variables

```java
@Test
void testWithMultipleVariables() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    new EnvironmentVariables(
        "API_URL", "https://test-api.example.com",
        "API_KEY", "test-key-12345",
        "TIMEOUT", "5000"
    ).execute(() -> {
        assertEquals("https://test-api.example.com", System.getenv("API_URL"));
        assertEquals("test-key-12345", System.getenv("API_KEY"));
        assertEquals("5000", System.getenv("TIMEOUT"));
        
        // Test your configuration loading
        AppConfig config = new AppConfig();
        assertEquals(5000, config.getTimeout());
    });
}
```

#### Fluent API with and()

```java
@Test
void testFluentAPI() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    new EnvironmentVariables("DATABASE_URL", "jdbc:postgresql://localhost/test")
        .and("DATABASE_USER", "testuser")
        .and("DATABASE_PASSWORD", "testpass")
        .and("DATABASE_POOL_SIZE", "5")
        .execute(() -> {
            // All variables are set
            DatabaseConfig dbConfig = new DatabaseConfig();
            assertEquals("testuser", dbConfig.getUser());
            assertEquals("testpass", dbConfig.getPassword());
            assertEquals(5, dbConfig.getPoolSize());
        });
}
```

### Construction Options

#### 1. Default Constructor (Empty)

```java
EnvironmentVariables envVars = new EnvironmentVariables();
// Later: envVars.set("VAR", "value");
```

#### 2. Name-Value Pairs

```java
EnvironmentVariables envVars = new EnvironmentVariables(
    "VAR1", "value1",
    "VAR2", "value2",
    "VAR3", "value3"
);
```

#### 3. From Properties

```java
Properties props = new Properties();
props.load(new FileInputStream("test.properties"));
EnvironmentVariables envVars = new EnvironmentVariables(props);
```

#### 4. From Map

```java
Map<String, String> config = new HashMap<>();
config.put("ENV", "test");
config.put("DEBUG", "true");
EnvironmentVariables envVars = new EnvironmentVariables(config);
```

### Immutable vs Mutable Operations

#### Immutable: and() Creates New Instance

```java
EnvironmentVariables base = new EnvironmentVariables("VAR1", "value1");
EnvironmentVariables extended = base.and("VAR2", "value2");

// base is unchanged
assertEquals(1, base.getVariables().size());

// extended has both variables
assertEquals(2, extended.getVariables().size());
```

**Use `and()` when:**
- Building test fixtures incrementally
- Creating variations of base configuration
- Sharing common variables across tests

#### Mutable: set() and remove() Modify Instance

```java
EnvironmentVariables envVars = new EnvironmentVariables();
envVars.set("VAR1", "value1");      // Add variable
envVars.set("VAR1", "new_value");   // Update variable
envVars.remove("VAR1");              // Remove variable
```

**Use `set()`/`remove()` when:**
- Building configuration step by step
- Modifying variables during test setup
- Conditional variable configuration

### Advanced Patterns

#### Nested Execution Contexts

```java
@Test
void testNestedContexts() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    new EnvironmentVariables("OUTER", "outer_value")
        .execute(() -> {
            assertEquals("outer_value", System.getenv("OUTER"));
            
            new EnvironmentVariables("INNER", "inner_value")
                .execute(() -> {
                    assertEquals("outer_value", System.getenv("OUTER"));
                    assertEquals("inner_value", System.getenv("INNER"));
                });
            
            // Inner context cleaned up
            assertEquals("outer_value", System.getenv("OUTER"));
            assertNull(System.getenv("INNER"));
        });
}
```

#### Removing Variables (null values)

```java
@Test
void testRemovingVariable() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    // Set a variable to null to remove it from the environment
    new EnvironmentVariables("REMOVED_VAR", null)
        .execute(() -> {
            // Variable is not visible
            assertNull(System.getenv("REMOVED_VAR"));
        });
}
```

#### Overriding System Variables

```java
@Test
void testOverrideSystemVariable() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    String originalPath = System.getenv("PATH");
    
    new EnvironmentVariables("PATH", "/custom/test/path")
        .execute(() -> {
            assertEquals("/custom/test/path", System.getenv("PATH"));
            
            // Test code that depends on custom PATH
        });
    
    // Original PATH restored
    assertEquals(originalPath, System.getenv("PATH"));
}
```

#### Conditional Variable Setup

```java
@Test
void testConditionalSetup() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    EnvironmentVariables envVars = new EnvironmentVariables("BASE_VAR", "base");
    
    if (needsDebugMode()) {
        envVars.set("DEBUG", "true");
        envVars.set("LOG_LEVEL", "DEBUG");
    }
    
    if (needsAuthentication()) {
        envVars.set("AUTH_TOKEN", "test-token");
    }
    
    envVars.execute(() -> {
        // Test with conditionally configured environment
    });
}
```

### Complete Real-World Examples

#### Example 1: Testing Configuration Loading

```java
class ConfigurationTest {
    
    @Test
    void testProductionConfig() throws Exception {
        // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
        EnvironmentVariableMocker.initMocked();
        
        new EnvironmentVariables(
            "ENV", "production",
            "API_URL", "https://api.example.com",
            "API_KEY", "prod-key-xyz",
            "CACHE_TTL", "3600",
            "MAX_CONNECTIONS", "100"
        ).execute(() -> {
            AppConfig config = AppConfig.fromEnvironment();
            
            assertEquals("production", config.getEnvironment());
            assertEquals("https://api.example.com", config.getApiUrl());
            assertTrue(config.isCacheEnabled());
            assertEquals(3600, config.getCacheTtl());
            assertEquals(100, config.getMaxConnections());
        });
    }
    
    @Test
    void testDevelopmentConfig() throws Exception {
        // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
        EnvironmentVariableMocker.initMocked();
        
        new EnvironmentVariables(
            "ENV", "development",
            "API_URL", "http://localhost:8080",
            "DEBUG", "true"
        ).execute(() -> {
            AppConfig config = AppConfig.fromEnvironment();
            
            assertEquals("development", config.getEnvironment());
            assertTrue(config.isDebugEnabled());
        });
    }
    
    @Test
    void testMissingRequiredVariable() throws Exception {
        // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
        EnvironmentVariableMocker.initMocked();
        
        new EnvironmentVariables("ENV", "test")
            .execute(() -> {
                // API_URL is required but missing
                assertThrows(ConfigurationException.class, () -> {
                    AppConfig.fromEnvironment();
                });
            });
    }
}
```

#### Example 2: Testing Feature Flags

```java
class FeatureFlagTest {
    
    @Test
    void testFeatureEnabled() throws Exception {
        // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
        EnvironmentVariableMocker.initMocked();
        
        new EnvironmentVariables(
            "FEATURE_NEW_UI", "true",
            "FEATURE_BETA_API", "true"
        ).execute(() -> {
            FeatureFlags flags = new FeatureFlags();
            
            assertTrue(flags.isNewUiEnabled());
            assertTrue(flags.isBetaApiEnabled());
            
            // Test code path when features are enabled
            assertNotNull(new NewUIController());
        });
    }
    
    @Test
    void testFeatureDisabled() throws Exception {
        // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
        EnvironmentVariableMocker.initMocked();
        
        new EnvironmentVariables(
            "FEATURE_NEW_UI", "false"
        ).execute(() -> {
            FeatureFlags flags = new FeatureFlags();
            
            assertFalse(flags.isNewUiEnabled());
            
            // Test fallback to old UI
            assertNotNull(new LegacyUIController());
        });
    }
}
```

#### Example 3: Testing Database Connection

```java
class DatabaseConnectionTest {
    
    @Test
    void testPostgresConnection() throws Exception {
        // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
        EnvironmentVariableMocker.initMocked();
        
        new EnvironmentVariables(
            "DB_HOST", "localhost",
            "DB_PORT", "5432",
            "DB_NAME", "testdb",
            "DB_USER", "testuser",
            "DB_PASSWORD", "testpass",
            "DB_SSL", "false"
        ).execute(() -> {
            DatabaseConfig config = DatabaseConfig.fromEnvironment();
            
            assertEquals("jdbc:postgresql://localhost:5432/testdb", 
                config.getJdbcUrl());
            assertEquals("testuser", config.getUsername());
            assertFalse(config.isSslEnabled());
        });
    }
    
    @Test
    void testConnectionPooling() throws Exception {
        // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
        EnvironmentVariableMocker.initMocked();
        
        new EnvironmentVariables(
            "DB_POOL_SIZE", "10",
            "DB_POOL_TIMEOUT", "30000",
            "DB_POOL_MAX_LIFETIME", "1800000"
        ).execute(() -> {
            PoolConfig pool = PoolConfig.fromEnvironment();
            
            assertEquals(10, pool.getMaxPoolSize());
            assertEquals(30000, pool.getConnectionTimeout());
            assertEquals(1800000, pool.getMaxLifetime());
        });
    }
}
```

#### Example 4: Testing Multi-Environment Setup

```java
class MultiEnvironmentTest {
    
    private EnvironmentVariables baseConfig() {
        return new EnvironmentVariables(
            "APP_NAME", "MyApp",
            "APP_VERSION", "1.0.0"
        );
    }
    
    @Test
    void testDevelopmentEnvironment() throws Exception {
        // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
        EnvironmentVariableMocker.initMocked();
        
        baseConfig()
            .and("ENV", "development")
            .and("DEBUG", "true")
            .and("LOG_LEVEL", "DEBUG")
            .execute(() -> {
                Environment env = Environment.current();
                assertEquals("development", env.getName());
                assertTrue(env.isDebugMode());
            });
    }
    
    @Test
    void testStagingEnvironment() throws Exception {
        // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
        EnvironmentVariableMocker.initMocked();
        
        baseConfig()
            .and("ENV", "staging")
            .and("DEBUG", "false")
            .and("LOG_LEVEL", "INFO")
            .and("MONITORING", "true")
            .execute(() -> {
                Environment env = Environment.current();
                assertEquals("staging", env.getName());
                assertFalse(env.isDebugMode());
                assertTrue(env.isMonitoringEnabled());
            });
    }
    
    @Test
    void testProductionEnvironment() throws Exception {
        // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
        EnvironmentVariableMocker.initMocked();
        
        baseConfig()
            .and("ENV", "production")
            .and("DEBUG", "false")
            .and("LOG_LEVEL", "WARN")
            .and("MONITORING", "true")
            .and("SECURITY", "strict")
            .execute(() -> {
                Environment env = Environment.current();
                assertEquals("production", env.getName());
                assertFalse(env.isDebugMode());
                assertEquals("strict", env.getSecurityLevel());
            });
    }
}
```

### Low-Level API: EnvironmentVariableMocker

For advanced use cases, you can use the `EnvironmentVariableMocker` directly:

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.util.EnvironmentVariableMocker;

@BeforeAll
static void setupMocking() {
    EnvironmentVariableMocker.initMocked();
}

@AfterAll
static void teardownMocking() {
    EnvironmentVariableMocker.deinitMocked();
}

@Test
void testWithDirectMocking() {
    Map<String, String> env = new HashMap<>();
    env.put("TEST_VAR", "test_value");
    
    EnvironmentVariableMocker.connect(env);
    
    try {
        assertEquals("test_value", System.getenv("TEST_VAR"));
        // Your test code
    } finally {
        EnvironmentVariableMocker.pop();
    }
}
```

**Direct Mocker Features:**
- Stack-based environment management
- Support for nested contexts
- Null value handling (removes variables)
- Thread-safe operation
- Platform-specific environment block generation

### Best Practices

#### 1. Use EnvironmentVariables for Most Cases

```java
// ✅ Good: Clear and self-documenting
new EnvironmentVariables("API_KEY", "test-key").execute(() -> {
    // test code
});

// ❌ Avoid: Direct mocker usage unless needed
EnvironmentVariableMocker.connect(map);
// ...
EnvironmentVariableMocker.pop();
```

#### 2. Keep Variable Scope Minimal

```java
// ✅ Good: Variables only exist during test
@Test
void test() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    new EnvironmentVariables("VAR", "value").execute(() -> {
        // test code
    });
}

// ❌ Avoid: Class-level variables affect all tests
```

#### 3. Use Descriptive Variable Names

```java
// ✅ Good: Clear purpose
new EnvironmentVariables(
    "DATABASE_CONNECTION_TIMEOUT_MS", "5000",
    "API_RETRY_MAX_ATTEMPTS", "3"
)

// ❌ Avoid: Ambiguous names
new EnvironmentVariables("TIMEOUT", "5000", "RETRIES", "3")
```

#### 4. Document Required vs Optional Variables

```java
@Test
void testConfiguration() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    new EnvironmentVariables(
        // Required variables
        "API_URL", "https://api.example.com",
        "API_KEY", "test-key",
        
        // Optional variables (with defaults in code)
        "TIMEOUT", "5000",
        "DEBUG", "true"
    ).execute(() -> {
        // test code
    });
}
```

#### 5. Test Both Presence and Absence

```java
@Test
void testWithVariable() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    new EnvironmentVariables("FEATURE_FLAG", "true")
        .execute(() -> {
            assertTrue(Features.isEnabled("FEATURE_FLAG"));
        });
}

@Test
void testWithoutVariable() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    new EnvironmentVariables()
        .execute(() -> {
            // Variable not set - should use default
            assertFalse(Features.isEnabled("FEATURE_FLAG"));
        });
}
```

### Common Pitfalls and Solutions

#### Pitfall 1: Forgetting to Initialize Mocker

```java
// ❌ Error: Mocking not initialized
@Test
void test() throws Exception {
    new EnvironmentVariables("VAR", "value").execute(() -> {
        // This will fail!
    });
}

// ✅ Solution: Initialize in @BeforeAll
@BeforeAll
static void setup() {
    EnvironmentVariableMocker.initMocked();
}
```

#### Pitfall 2: Variable Persists After Test

```java
// ❌ Problem: Manual cleanup forgotten
@Test
void test() {
    Map<String, String> env = new HashMap<>();
    env.put("VAR", "value");
    EnvironmentVariableMocker.connect(env);
    // Forgot to call pop()!
}

// ✅ Solution: Use EnvironmentVariables.execute()
@Test
void test() throws Exception {
    // Initialize the mocker once before first use (typically in @BeforeEach or @BeforeAll)
    EnvironmentVariableMocker.initMocked();
    
    new EnvironmentVariables("VAR", "value")
        .execute(() -> {
            // Automatic cleanup
        });
}
```

#### Pitfall 3: Duplicate Variable Names with and()

```java
// ❌ Error: Throws IllegalArgumentException
EnvironmentVariables env = new EnvironmentVariables("VAR", "value1");
env.and("VAR", "value2");  // Exception!

// ✅ Solution: Use set() to overwrite
EnvironmentVariables env = new EnvironmentVariables("VAR", "value1");
env.set("VAR", "value2");  // OK - overwrites
```

### Testing Tips

1. **Isolate Tests**: Each test should set up its own environment variables
2. **Clean State**: Use `execute()` to ensure cleanup
3. **Test Defaults**: Verify behavior when variables are missing
4. **Test Invalid Values**: Check error handling for malformed configuration
5. **Document Dependencies**: List all environment variables your code expects

### See Also

- **`EnvironmentVariablesTest.java`** - Comprehensive test examples
- **`EnvironmentVariableMockerTest.java`** - Low-level mocking tests
- [System Stubs](https://github.com/webcompere/system-stubs) - The underlying mocking library

