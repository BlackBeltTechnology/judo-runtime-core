# JUDO Testkit - Test Configuration Guide

## Quick Start for Dependent Projects

When using the JUDO testkit in your project, follow these steps to ensure proper configuration.

### 1. Add Dependencies to Your Test Module

In your test module's `pom.xml` (e.g., `application/app/pom.xml`), add:

```xml
<dependencies>
    <!-- REQUIRED: Generated DAO classes -->
    <dependency>
        <groupId>your.project.groupId</groupId>
        <artifactId>your-project-dao</artifactId>
        <version>${project.version}</version>
        <scope>test</scope>
    </dependency>
    
    <!-- REQUIRED: Model files -->
    <dependency>
        <groupId>your.project.groupId</groupId>
        <artifactId>your-project-model</artifactId>
        <version>${project.version}</version>
        <scope>test</scope>
    </dependency>
    
    <!-- REQUIRED: JUDO testkit -->
    <dependency>
        <groupId>hu.blackbelt.judo.runtime</groupId>
        <artifactId>judo-runtime-core-guice-testkit</artifactId>
        <version>${judo.version}</version>
        <scope>test</scope>
    </dependency>
    
    <!-- OPTIONAL: For Guice module customization -->
    <dependency>
        <groupId>com.google.inject</groupId>
        <artifactId>guice</artifactId>
        <version>5.1.0</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

### 2. Ensure Model Files Are in Classpath

Your model module should package model files in `/model/` directory:

**Option A: Copy model files in model module's `pom.xml`:**

```xml
<build>
    <resources>
        <resource>
            <directory>${project.build.directory}/generated-sources/model</directory>
            <targetPath>model</targetPath>
            <includes>
                <include>**/*.model</include>
                <include>**/*.xml</include>
            </includes>
        </resource>
    </resources>
</build>
```

**Option B: Use maven-resources-plugin:**

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-resources-plugin</artifactId>
    <executions>
        <execution>
            <id>copy-models</id>
            <phase>process-resources</phase>
            <goals>
                <goal>copy-resources</goal>
            </goals>
            <configuration>
                <outputDirectory>${project.build.outputDirectory}/model</outputDirectory>
                <resources>
                    <resource>
                        <directory>${project.build.directory}/generated-sources/model</directory>
                        <includes>
                            <include>**/*.model</include>
                            <include>**/*.xml</include>
                        </includes>
                    </resource>
                </resources>
            </configuration>
        </execution>
    </executions>
</plugin>
```

### 3. Configure Build Order

Ensure modules are built in the correct order in your parent `pom.xml`:

```xml
<modules>
    <module>model</module>      <!-- 1. Generate models first -->
    <module>dao</module>        <!-- 2. Generate DAOs -->
    <module>app</module>        <!-- 3. Application/tests last -->
</modules>
```

### 4. Write Your Test

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.ReferenceInjector;

class MyTest {
    
    @JudoTest(
        modelName = "yourmodel",
        modelSource = JudoTest.ModelSource.CLASSPATH  // Load from JAR
    )
    void testSomething(JudoRuntimeFixture fixture) {
        // Your test logic
        MyInterceptor interceptor = ReferenceInjector.createAndInject(
            MyInterceptor.class,
            fixture.getInjector()
        );
        
        // Test your interceptor/operation
        // Transaction automatically managed (AUTO_ROLLBACK by default)
    }
}
```

### 5. (Optional) Add Custom Guice Modules

For mocking external dependencies or providing test-specific bindings:

```java
import com.google.inject.AbstractModule;

// Define custom test module
public class MyTestModule extends AbstractModule {
    @Override
    protected void configure() {
        // Bind mocks or test implementations
        bind(EmailService.class).toInstance(Mockito.mock(EmailService.class));
        bind(PaymentGateway.class).to(TestPaymentGateway.class);
    }
}

// Use in test
@JudoTest(
    modelName = "yourmodel",
    modules = { MyTestModule.class }
)
void testWithMocks(JudoRuntimeFixture fixture) {
    // EmailService is now mocked
}
```

## Common Issues and Solutions

### Issue 1: "DAO modules not found"

**Symptom:**
```
ClassNotFoundException: your.project.dao.SomeDao
```

**Solution:**
Add the DAO module as a test dependency (see step 1 above).

### Issue 2: "Model not found"

**Symptom:**
```
IllegalArgumentException: Could not load model 'yourmodel'.
Filesystem error: Directory does not exist: target/generated-test-sources/model
Classpath error: Could not load model from classpath
```

**Solution:**
1. Add model module as test dependency
2. Ensure model files are packaged in `/model/` directory
3. Use `modelSource = ModelSource.CLASSPATH` in `@JudoTest`

### Issue 3: AbstractMethodError with Liquibase

**Symptom:**
```
AbstractMethodError: Receiver class StreamResourceAccessor does not define or inherit 
an implementation of 'abstract java.util.List getAll(java.lang.String)'
```

**Solution:**
This is a Liquibase version compatibility issue. Ensure you're using:
- JUDO Runtime Core version with fixed `StreamResourceAccessor` (includes `getAll()` method)
- Compatible Liquibase version (4.9.1+)

### Issue 4: Tests pass individually but fail in suite

**Symptom:**
Tests work when run individually but fail when run as a suite.

**Solution:**
Use `@TestMethodOrder` and proper transaction handling:

```java
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MyTest {
    
    @JudoTest(transaction = TransactionHandling.AUTO_ROLLBACK)
    void test1(JudoRuntimeFixture fixture) {
        // Each test gets clean database state
    }
}
```

## Build and Test Commands

### Build from root (recommended)
```bash
# Build entire project including model and DAO generation
cd /path/to/your/project
mvn clean install -DskipTests

# Run specific test
mvn test -pl application/app -Dtest=YourTest
```

### Build and test in one command
```bash
mvn clean install
```

### Run only failing test for debugging
```bash
mvn test -pl application/app -Dtest=YourTest#testMethod
```

## Configuration Options

### Model Source Strategies

Choose the appropriate `modelSource` based on your scenario:

| Strategy | When to Use | Configuration |
|----------|-------------|---------------|
| `AUTO` (default) | Development - tries filesystem first, falls back to classpath | `@JudoTest(modelName = "mymodel")` |
| `FILESYSTEM` | Local development only, models in `target/generated-test-sources/model` | `@JudoTest(modelSource = ModelSource.FILESYSTEM)` |
| `CLASSPATH` | CI/CD, packaged tests, model in JAR | `@JudoTest(modelSource = ModelSource.CLASSPATH)` |

### Transaction Handling Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `AUTO_ROLLBACK` (default) | Transaction started & rolled back automatically | Clean DB between tests, fastest |
| `AUTO_COMMIT` | Transaction committed, tables truncated after test | Verify persisted data |
| `MANUAL` | You control transaction lifecycle | Complex scenarios, savepoints |
| `NONE` | No transaction support | Read-only tests, maximum performance |

### Datasource Lifecycle Modes

| Mode | Scope | Isolation | Performance | Use Case |
|------|-------|-----------|-------------|----------|
| `BY_METHOD` (default) | Per test method | Maximum | Slowest | Each test needs clean DB |
| `BY_CLASS` | Per test class | Medium | Medium (>= 5× vs BY_METHOD on real models) | Tests in class don't conflict |
| `SINGLETON` | Shared across all classes | Minimum | Fastest | Read-only or well-isolated tests |

### Why runtime caching exists

Before the runtime cache was introduced, `BY_CLASS` and `SINGLETON` modes only
shared the `DataSource` and the `JudoModelLoader`. Every test method still:

1. Re-built the `QueryFactory` by extracting JQL expressions from the ASM
   model (~3 s on a real-world model).
2. Re-ran the Liquibase changelog re-validation against `DATABASECHANGELOG`
   (~5 s, including lock acquire / release).
3. Re-created the Guice `Injector` over `judoDefaultModule + databaseModule
   + customModule` (~10–15 s).
4. Re-resolved the `PlatformTransactionManager`.

Empirically on a 20-method class with a real-world model (~20 MB ASM), the
model-only cache yielded just ~10 % speed-up because the model load is only
~3 s of the ~28 s per-test cost. The remaining ~25 s is repeated Liquibase
work and Guice injector construction.

**Caching these four artifacts amortises them across the entire class scope**,
dropping per-method cost from ~26 s to under 1 ms once the cache is warm — a
>= 20× observed speed-up on real-world models. On a 20-method class this
saves roughly **8 minutes of wall-clock per test class**.

### Caching invariants (BY_CLASS / SINGLETON)

For `BY_CLASS` and `SINGLETON` modes, the testkit caches the derived runtime
artifacts — `QueryFactory`, the database `Module`, the Liquibase executor, the
Guice `Injector`, and the `PlatformTransactionManager` — so they are built
exactly once per scope and reused across every test method. This is what gives
`BY_CLASS` its >= 5× speed-up over `BY_METHOD` on real-world models.

This caching is **transparent** for typical tests (the `@JudoTest` API is
unchanged) but has a few consequences you must be aware of:

- **Stateful interceptor instances are reused across methods.** An interceptor
  registered via `interceptors = { ... }` is instantiated once per cached
  runtime; any field state it carries persists across all methods of the class.
  If your interceptor accumulates state (counters, captured calls, etc.)
  either reset it explicitly in `@BeforeEach`, or pin the test class to
  `BY_METHOD`.
- **Schema-mutating tests must use `BY_METHOD`.** If a test drops a table,
  alters a column, or otherwise mutates the database schema and expects the
  next method to see a fresh schema (because Liquibase would re-run), it
  *will break* under `BY_CLASS` or `SINGLETON`: Liquibase executes exactly
  once per cached runtime. Switch such tests to `BY_METHOD`.
- **Subclasses of `JudoRuntimeFixture` overriding `init(…)`** — the cached
  path bypasses `init(…)` entirely (it uses `prepareWithCachedRuntime` instead),
  so any subclass override of `init` is not invoked under `BY_CLASS` /
  `SINGLETON`. Use `BY_METHOD` if your test relies on a custom `init` override.
- **Cache key.** Two `@JudoTest` configurations differing only in `modules` or
  `interceptors` produce *different* cache entries and therefore do not share
  a cached runtime — you do not need to worry about cross-contamination from
  a similarly-named test class with different bindings.

**Escape hatches:** when the cache invariants above are a problem, you have
two options.

1. **Drop to `BY_METHOD`** — every method gets a fresh DataSource, fresh
   Liquibase migration, fresh `Injector`. Maximum isolation, slowest
   (you pay the full cold cost on every method):
   ```java
   @JudoTest(dataSourceMode = JudoTest.DataSourceMode.BY_METHOD)
   ```

2. **Keep the shared DataSource and JudoModelLoader, but disable the runtime
   cache** with `cacheRuntime = false`. The DataSource is reused per class
   (BY_CLASS) or per JVM (SINGLETON), but the `Injector`, `QueryFactory`,
   `Module`, Liquibase executor, and `PlatformTransactionManager` are
   rebuilt for every method. This is the right choice when you want
   `BY_CLASS`-level perf on the datasource side but stateful interceptors
   / schema mutation / `init(…)` overrides on the runtime side.
   ```java
   @JudoTest(dataSourceMode = JudoTest.DataSourceMode.BY_CLASS,
             cacheRuntime = false)
   ```

### Behavior matrix

The `dataSourceMode` and `cacheRuntime` flags interact as follows:

| `dataSourceMode` | `cacheRuntime` | DataSource | Liquibase | Injector / QueryFactory / TxManager | JudoModelLoader |
|---|---|---|---|---|---|
| BY_METHOD | *(ignored)* | per method | per method | per method | per method |
| BY_CLASS | `true` (default) | per class | once | per class | per class |
| **BY_CLASS** | **`false`** | **per class** | **per method** | **per method** | **per class** |
| SINGLETON | *(ignored)* | JVM-wide | once | JVM-wide | JVM-wide |

The **bold** row is reachable only via `cacheRuntime = false`.

`cacheRuntime` only affects `BY_CLASS` mode. It is ignored for:
- `BY_METHOD` — nothing is cached regardless.
- `SINGLETON` — always cached. Disabling the cache on a JVM-wide DataSource
  would re-run Liquibase per method against a shared database, risking
  schema corruption under parallel execution.
- Method-level `@JudoTest` annotations (those always behave as `BY_METHOD`).

### Why `cacheRuntime = false` was added (and why only for BY_CLASS)

The runtime cache was originally unconditional for `BY_CLASS` and `SINGLETON`
— the only opt-out was dropping to `BY_METHOD`, which pays the full ~28 s
cold cost on every method. Three legitimate use cases needed an opt-out that
still preserved the per-class `DataSource` for performance:

1. **Stateful interceptor instances** — an interceptor registered via
   `interceptors = { … }` is instantiated once per cached runtime and
   accumulates state (counters, captured calls) across every method.
   Resetting in `@BeforeEach` works but is easy to forget.
2. **Schema-mutating tests** — a test that drops a table expects Liquibase
   to re-create it on the next method; with the cache, Liquibase only ran
   once per scope.
3. **Custom `JudoRuntimeFixture` subclasses overriding `init(…)`** — the
   cached path uses `prepareWithCachedRuntime(…)` and never invokes
   `init(…)` after the first method, silently skipping any subclass
   override.

`cacheRuntime = false` solves all three by rebuilding the `Injector`,
`QueryFactory`, Liquibase executor, and `PlatformTransactionManager` every
method, while keeping the per-class `DataSource` and `JudoModelLoader`
cached for performance.

**Why the flag is BY_CLASS-only:**

`SINGLETON + cacheRuntime = false` was considered and rejected because:

- **Liquibase race condition.** `SINGLETON` shares one `DataSource` JVM-wide.
  With the cache disabled, every method would re-run Liquibase against that
  shared database. Under JUnit parallel execution, multiple Liquibase
  instances would hit the same schema concurrently, risking
  `LockException`, corrupted `DATABASECHANGELOGLOCK`, or schema drift.
- **No coherent use case.** All three motivating scenarios are inherently
  per-class-or-narrower: stateful interceptors are class-scoped, schema
  mutations want fresh schemas (so `BY_METHOD` is correct anyway), custom
  `init(…)` overrides only make sense per-class. Nobody wants "JVM-wide
  `DataSource` sharing but per-method `Injector` rebuilds across unrelated
  test classes."
- **Misleading semantics.** Users reading `SINGLETON + cacheRuntime = false`
  would expect full per-method freshness and be surprised when they share
  state via the JVM-wide `DataSource`.

The routing predicate therefore silently ignores `cacheRuntime` for
`SINGLETON` (always caches). If you need per-method runtime freshness AND a
shared database, that's a contradiction — use `BY_CLASS + cacheRuntime =
false` (per-class `DataSource`) or `BY_METHOD` (everything fresh).

### Database Configuration

| Parameter | Values | Default | Override with Environment Variable |
|-----------|--------|---------|-------------------------------------|
| `dialect` | `hsqldb`, `postgresql` | `hsqldb` | `JUDO_TEST_DIALECT` |
| `container` | `none`, `postgresql`, `yugabytedb` | `none` | `JUDO_TEST_CONTAINER` |

## Example Project Structure

```
your-project/
├── pom.xml (parent)
├── model/
│   ├── pom.xml
│   └── src/
│       └── main/
│           └── resources/
│               └── model/          ← Model files packaged here
│                   ├── yourmodel-asm.model
│                   ├── yourmodel-rdbms_hsqldb.model
│                   └── ...
├── dao/
│   ├── pom.xml
│   └── target/
│       └── generated-sources/      ← Generated DAO classes
└── app/
    ├── pom.xml                     ← Add dependencies here
    └── src/
        └── test/
            └── java/
                └── MyTest.java     ← Your tests
```

## Environment Variables for Database Configuration

You can override the database dialect and container using environment variables:

```bash
# Use PostgreSQL for all tests
export JUDO_TEST_DIALECT=postgresql
export JUDO_TEST_CONTAINER=postgresql
mvn test

# Use YugabyteDB for all tests
export JUDO_TEST_DIALECT=postgresql
export JUDO_TEST_CONTAINER=yugabytedb
mvn test

# Use in-line for CI/CD
JUDO_TEST_DIALECT=postgresql JUDO_TEST_CONTAINER=postgresql mvn test
```

**Configuration Priority:**
1. Environment variables (`JUDO_TEST_DIALECT`, `JUDO_TEST_CONTAINER`) - highest priority
2. Annotation parameters (`dialect`, `container`)
3. Smart defaults (HSQLDB in-memory)

**Available Options:**

| Environment Variable | Values | Default |
|---------------------|--------|---------|
| `JUDO_TEST_DIALECT` | `hsqldb`, `postgresql` | `hsqldb` |
| `JUDO_TEST_CONTAINER` | `none`, `postgresql`, `yugabytedb` | `none` |

**Auto-detection:** When `JUDO_TEST_DIALECT=postgresql` and no container is specified, automatically uses PostgreSQL container.

## Maven Surefire Configuration (Optional)

For additional control, configure maven-surefire-plugin:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>3.0.0-M9</version>
    <configuration>
        <environmentVariables>
            <!-- Override database configuration -->
            <JUDO_TEST_DIALECT>postgresql</JUDO_TEST_DIALECT>
            <JUDO_TEST_CONTAINER>postgresql</JUDO_TEST_CONTAINER>
        </environmentVariables>
        
        <!-- Ensure proper test execution order -->
        <runOrder>alphabetical</runOrder>
        
        <!-- Fork for test isolation -->
        <forkCount>1</forkCount>
        <reuseForks>true</reuseForks>
    </configuration>
</plugin>
```

## Complete Example

### pom.xml (test module)
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    
    <parent>
        <groupId>com.example</groupId>
        <artifactId>myapp-parent</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    
    <artifactId>myapp-app</artifactId>
    
    <dependencies>
        <!-- Your runtime dependencies -->
        
        <!-- Test dependencies -->
        <dependency>
            <groupId>com.example</groupId>
            <artifactId>myapp-dao</artifactId>
            <version>${project.version}</version>
            <scope>test</scope>
        </dependency>
        
        <dependency>
            <groupId>com.example</groupId>
            <artifactId>myapp-model</artifactId>
            <version>${project.version}</version>
            <scope>test</scope>
        </dependency>
        
        <dependency>
            <groupId>hu.blackbelt.judo.runtime</groupId>
            <artifactId>judo-runtime-core-guice-testkit</artifactId>
            <version>${judo.version}</version>
            <scope>test</scope>
        </dependency>
        
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

### Test Class Examples

**Example 1: Basic Test with Auto-Rollback (Default)**
```java
package com.example.myapp.test;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.ReferenceInjector;
import com.example.myapp.interceptors.MyInterceptor;

import static org.junit.jupiter.api.Assertions.*;

class MyInterceptorTest {

    @JudoTest(
        modelName = "myapp",
        modelSource = JudoTest.ModelSource.CLASSPATH
    )
    void testMyInterceptor(JudoRuntimeFixture fixture) {
        // Create and inject dependencies
        MyInterceptor interceptor = ReferenceInjector.createAndInject(
            MyInterceptor.class,
            fixture.getInjector()
        );
        
        // Test your logic
        Payload input = Payload.map("name", "Test");
        Object result = interceptor.process(input);
        
        assertNotNull(result);
        // Transaction automatically rolled back - clean DB state
    }
}
```

**Example 2: PostgreSQL Container (via Environment Variable)**
```bash
# Set environment variables
export JUDO_TEST_DIALECT=postgresql
export JUDO_TEST_CONTAINER=postgresql

# Run tests - all @JudoTest tests will use PostgreSQL
mvn test
```

**Example 3: Class-Level Configuration with Shared Datasource**
```java
@JudoTest(
    modelName = "myapp",
    modelSource = JudoTest.ModelSource.CLASSPATH,
    dataSourceMode = JudoTest.DataSourceMode.BY_CLASS,
    transaction = JudoTest.TransactionHandling.AUTO_ROLLBACK
)
class MyIntegrationTests {
    
    @Test
    void test1(JudoRuntimeFixture fixture) {
        // Uses shared class-level datasource
        // Transaction rolled back after test
    }
    
    @Test
    void test2(JudoRuntimeFixture fixture) {
        // Same datasource as test1
        // Transaction rolled back after test
    }
}
```

**Example 4: Custom Guice Modules**
```java
import com.google.inject.AbstractModule;

// Custom test module
public class MyTestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(EmailService.class).toInstance(Mockito.mock(EmailService.class));
    }
}

// Use in test
class MyTest {
    @JudoTest(
        modelName = "myapp",
        modules = { MyTestModule.class }
    )
    void testWithMocks(JudoRuntimeFixture fixture) {
        // EmailService is now mocked
        MyInterceptor interceptor = ReferenceInjector.createAndInject(
            MyInterceptor.class,
            fixture.getInjector()
        );
        
        // Test with mocked dependencies
    }
}
```

## Troubleshooting Checklist

- [ ] DAO module added as test dependency
- [ ] Model module added as test dependency  
- [ ] Model files packaged in `/model/` directory in model JAR
- [ ] Build order correct (model → dao → app)
- [ ] Using `ModelSource.CLASSPATH` for packaged tests
- [ ] Correct model name in `@JudoTest` annotation
- [ ] All modules built with `mvn clean install`

## Need Help?

See also:
- `README.md` - Main testkit documentation
- `InterceptorIntegrationTest.java` - Complete example tests
- `JudoTestAnnotationExamples.java` - All `@JudoTest` features
