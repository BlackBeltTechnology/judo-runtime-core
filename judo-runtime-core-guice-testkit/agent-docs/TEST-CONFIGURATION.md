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

2. **Opt into the cached fast path** with `shareInjector = true`. The
   `Injector`, `QueryFactory`, `Module`, Liquibase executor, and
   `PlatformTransactionManager` are built ONCE per scope (per class for
   `BY_CLASS`; JVM-wide for `SINGLETON`) and reused across every test
   method. This is the right choice for read-heavy suites or any class
   where you've satisfied yourself there is no behavioural sharing risk
   (see [§ Behavioural sharing](#behavioural-sharing)).
   ```java
   @JudoTest(dataSourceMode = JudoTest.DataSourceMode.BY_CLASS,
             shareInjector = true)
   ```

### Behavior matrix

The `dataSourceMode` and `shareInjector` flags interact as follows:

| `dataSourceMode` | `shareInjector` | DataSource | Liquibase | Injector / QueryFactory / TxManager | JudoModelLoader |
|---|---|---|---|---|---|
| BY_METHOD | *(ignored)* | per method | per method | per method | per method |
| BY_CLASS | `false` *(default)* | per class | per method (no-op after 1st) | per method | per class |
| **BY_CLASS** | **`true`** | **per class** | **once** | **per class** | **per class** |
| SINGLETON | `false` *(default)* | JVM-wide | per method (no-op after 1st) | per method | JVM-wide |
| **SINGLETON** | **`true`** | **JVM-wide** | **once** | **JVM-wide** | **JVM-wide** |

The **bold** rows are the cached fast paths, reachable only via
`shareInjector = true`.

`shareInjector` is honoured uniformly for every class-scoped mode. It is
ignored only when there is no class scope to attach the cache to:
- `BY_METHOD` — nothing is cached regardless.
- Method-level `@JudoTest` annotations — those always behave as `BY_METHOD`.

### Behavioural sharing

> Renamed and inverted in the `share-injector-opt-in` change. The legacy
> `cacheRuntime` element (default `true`) has been removed; the replacement
> is `shareInjector` with default `false`.

**Why the default flipped.** Selecting `BY_CLASS` (or `SINGLETON`) used to
automatically grant *behavioural* sharing as well as *resource* sharing.
That conflated two orthogonal concerns:

- **Resource lifecycle** — how long does the physical `DataSource` live?
  Controlled by `dataSourceMode`. Sharing a `DataSource` is a pure
  performance win.
- **Behavioural sharing** — do two test methods receive the same `Injector`
  (and consequently the same `QueryFactory`, interceptor instances,
  Liquibase high-water-mark)? Controlled by `shareInjector`. This affects
  test correctness, not just speed.

Making behavioural sharing opt-in (rather than implicit in the mode
choice) matches pytest fixtures, JUnit 5 `PER_METHOD` instance lifecycle,
and Testcontainers — isolation by default; sharing on request.

**When to set `shareInjector = true`:**

- Performance: heavy-model classes with many methods (the 5×–10× win on
  rackinspect-scale models).
- Cross-method state you intentionally want — e.g. a `@BeforeAll`-built
  counter or cache.
- Read-only test suites where you've verified there is no mutation across
  methods.

**When to keep the default `shareInjector = false`:**

- Tests with stateful interceptor instances — each method gets a fresh
  interceptor with no inherited state.
- Schema-mutating tests — each method gets a fresh Liquibase run.
  Liquibase is idempotent via `DATABASECHANGELOG`, so this is cheap on
  cached-datasource modes.
- Tests using a `JudoRuntimeFixture` subclass overriding `init(…)` — the
  cold path always calls `init(…)`, your override fires on every method.
- By default — isolation between methods is the conservative choice.

**Why `SINGLETON + shareInjector = false` is safe.** The earlier design
considered this configuration dangerous because Liquibase would re-run
against a shared JVM-wide database. Re-analysis showed:

1. `DATABASECHANGELOG` records every applied changeset, so subsequent runs
   are no-ops (idempotence guaranteed by Liquibase itself).
2. `DATABASECHANGELOGLOCK` serialises concurrent invocations against the
   same physical schema, eliminating the "two workers race to apply" path.
3. The actual cost is a per-method round-trip to `DATABASECHANGELOG` plus
   `Injector` reconstruction — performance, not correctness.

So `SINGLETON + shareInjector = false` is now a fully valid default
configuration. Users who want maximum perf still opt into
`shareInjector = true`.

### Per-method overhead under `shareInjector = false`

"Idempotent" is not "free". When `shareInjector = false` against a shared
`DataSource` (`BY_CLASS` or `SINGLETON`), every test method pays a
reconstruction cost that the cached fast path skips:

| Cost component | What it costs | Per-method? |
|---|---|---|
| `DATABASECHANGELOG` SELECT | one query against the changelog tracking table | yes |
| `DATABASECHANGELOGLOCK` acquire / release | two row writes (HSQLDB) or row-level lock dance (PostgreSQL) | yes |
| Changeset "already applied" diff | in-memory comparison of changeset checksums vs DB rows | yes |
| Guice `Injector` construction | full module graph rebuild | yes |
| `QueryFactory` extraction | walks the ASM model to compile JQL expressions | yes |
| `PlatformTransactionManager` resolution | one injector lookup | yes |
| Model load (ASM, Liquibase model) | **no — the `JudoModelLoader` is still cached** | no |
| Liquibase changeset *application* | **no — idempotent skip after first method** | no |

#### Order-of-magnitude estimates

These numbers are **estimates pending end-to-end profiling**. They are
derived from the existing `RackinspectModelClassCachePerformanceTest`
baseline (28.7 s cold first method, ~26 s cached avg under the model-only
cache that preceded JNG-6374) and the design.md claim that the
model-loader portion is ~3 s of the cold cost. The breakdown is
approximate:

| Phase | Cold (uncached) | `shareInjector = false`, method N | `shareInjector = true`, method N |
|---|---|---|---|
| Model load | ~3 s | 0 ms (model cached) | 0 ms |
| Liquibase + `DATABASECHANGELOG` | ~6 s | ~50–200 ms (idempotent no-op + lock) | 0 ms |
| `Injector` + module rebuild | ~15 s | ~15 s | 0 ms |
| `QueryFactory` extraction | ~3 s | ~3 s | 0 ms |
| Other (TxManager, plumbing) | ~1 s | ~1 s | 0 ms |
| **Estimated per-method total** | **~28 s** | **~19 s** | **~0.1 s** |

The headline read: `shareInjector = false` saves the ~3 s model load and
the ~6 s Liquibase application, but pays ~15 s every method for injector
rebuild. **`shareInjector = true` is the only path to sub-second
per-method cost on a heavy model.**

> These figures are derived from the rackinspect baseline (~20 MB ASM,
> ~100-changeset Liquibase model). On smaller models the absolute numbers
> shrink, but the *ratio* `shareInjector = true` : `false` : `BY_METHOD`
> stays roughly 1 : 100 : 200.

#### When the overhead matters

| Suite shape | Recommendation |
|---|---|
| 1–3 methods per class | Default (`false`) — isolation wins, overhead negligible at low N |
| 4–10 methods per class | Default (`false`) — you pay ~1.5–3 minutes of overhead vs cached fast path; usually acceptable |
| 11–20+ methods per class | Consider `shareInjector = true` if no behavioural-isolation risk — you're paying ~5–10 minutes of overhead vs the cached path |
| Heavy-model perf benchmark | `shareInjector = true` is the only configuration whose results are meaningful |
| Read-only suite | `shareInjector = true` — no mutation, no risk, all the perf |
| Schema-mutating / stateful interceptor suite | `false` (default) — the overhead is the price of correctness |

#### A small note on the Liquibase lock

Under PostgreSQL with parallel-class JUnit execution
(`-DforkCount=N -Dthreads=N`) and `SINGLETON + shareInjector = false`,
the per-method `DATABASECHANGELOGLOCK` acquire / release path is the most
likely contention point. The lock is serialised correctly (no corruption
risk per design.md D3), but two workers racing to call `init.execute(...)`
will serialise on it, eroding parallelism. If you see
flat-line CPU on a parallel build with SINGLETON + default `shareInjector`,
this is the suspect. Fix: opt into `shareInjector = true` for the
performance-critical SINGLETON classes.

#### Pending measurement

The order-of-magnitude estimates above are derived from the cold-path
baseline; they have **not yet been benchmarked end-to-end** against the
idempotent re-run path. A follow-up task is tracked in
`docs/JNG-6374-testkit-runtime-cache.md` to run a 20-method
`@JudoTest(BY_CLASS)` with no `shareInjector` against rackinspect on CI
hardware and record actual per-method numbers. If the measured overhead
is materially larger than the estimates above, this section will be
updated and the recommendations re-tuned.

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
