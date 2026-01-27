# Interceptor Testing Implementation Plan

## Overview

This document outlines the implementation plan for adding interceptor testing capabilities to `judo-runtime-core-guice-testkit`. The goal is to enable developers to test `OperationCallInterceptor` implementations both in isolation (unit testing) and integrated with the dispatcher flow (integration testing).

## Current State

### Existing Support

The testkit already has **partial support** for interceptor testing:

1. **`ReferenceInjector`** - Utility to inject dependencies into interceptor instances
2. **`InterceptorIntegrationTest`** - Example showing manual interceptor testing pattern
3. **`@JudoTest(modules = {...})`** - Allows custom Guice modules

### Gap Analysis

The current approach requires **manual setup** and doesn't integrate interceptors into the actual dispatcher flow for end-to-end testing. There are two testing scenarios:

| Scenario | Description | Status |
|----------|-------------|--------|
| Unit Testing | Test interceptor logic in isolation (call `preCall`/`postCall` directly) | Supported |
| Integration Testing | Test interceptor within dispatcher flow (automatically invoked) | **Not Supported** |

## Architecture

### Interceptor Execution Flow

```
  Test Code                    Dispatcher                      BehaviourCall
 +---------+                 +-----------+                  +----------------+
 |         |  call operation |           |  routes to       |                |
 |  User   |---------------->| Dispatcher |---------------->| CreateInstance |
 |  Test   |                 |           |                  | Call           |
 +---------+                 +-----------+                  +-------+--------+
                                                                    |
                                                                    | uses
                                                                    v
                              +---------------------------------------------------+
                              |              CallInterceptorUtil<P, R>             |
                              |  +---------------------------------------------+  |
                              |  |  preCallInterceptors(input) -> modified input  |
                              |  |  shouldCallOriginal() -> boolean               |
                              |  |  postCallInterceptors(input, result) -> result |
                              |  +---------------------------------------------+  |
                              +------------------------+--------------------------+
                                                       |
                                                       | iterates
                                                       v
                              +----------------------------------------------------+
                              |          OperationCallInterceptorProvider          |
                              |  +----------------------------------------------+  |
                              |  |  getInterceptorsForOperation(asmModel, op)   |  |
                              |  |         v                                    |  |
                              |  |  List<OperationCallInterceptor>              |  |
                              |  +----------------------------------------------+  |
                              +----------------------------------------------------+
                                                       |
                                                       | returns
                                                       v
                              +----------------------------------------------------+
                              |             OperationCallInterceptor               |
                              |  +----------------------------------------------+  |
                              |  |  getName()                                   |  |
                              |  |  getOperations(asmModel) -> Collection       |  |
                              |  |  async() -> boolean                          |  |
                              |  |  terminateOnException() -> boolean           |  |
                              |  |  ignoreDecoratedCall() -> boolean            |  |
                              |  |  preCall(operation, input) -> modifiedInput  |  |
                              |  |  postCall(operation, input, result) -> result|  |
                              |  +----------------------------------------------+  |
                              +----------------------------------------------------+
```

### The Core Challenge: Dependency Injection Timing

```
  Current Flow (JudoRuntimeFixture):
  
  +------------------+     +------------------+     +------------------+
  |   1. prepare()   |---->|    2. init()     |---->|  3. Injector     |
  |   Load model,    |     |  Build modules   |     |     created      |
  |   init DB        |     |                  |     |                  |
  +------------------+     +--------+---------+     +------------------+
                                    |
                                    |
           +------------------------+------------------------+
           |                                                 |
           v                                                 v
  +------------------------+               +--------------------------------+
  |  JudoDefaultModule     |               |  OperationCallInterceptorProvider
  |  is built HERE with    |-------------->|  is bound HERE                 |
  |  all configurations    |               |  (either custom or default)    |
  +------------------------+               +--------------------------------+
                                                             |
                                                             v
                                             +----------------------------------+
                                             |  Dispatcher created with         |
                                             |  InterceptorProvider injected    |
                                             |  (IMMUTABLE after this point)    |
                                             +----------------------------------+
```

**The Problem:**
- Interceptors need `@Reference` dependencies (DAOs, services, etc.)
- These dependencies come from the Guice Injector
- BUT the Injector is created AFTER the InterceptorProvider is configured
- **Chicken-and-egg**: Need injector to inject into interceptors, but interceptors must be registered before injector creation

## Proposed Solution: Deferred Injection Pattern

The key insight is that we can **register interceptor instances before** the injector is created, and **inject their dependencies after**:

```
  Step 1: Create interceptor instances (no dependencies yet)
  
    MyInterceptor interceptor = new MyInterceptor();  // Fields are null
    
  Step 2: Register in TestInterceptorProvider (holds references)
  
    TestOperationCallInterceptorProvider provider = new ...();
    provider.addInterceptor(interceptor);
    
  Step 3: Build JudoDefaultModule with custom provider
  
    judoDefaultModuleBuilder.operationCallInterceptorProvider(provider)
    
  Step 4: Create Guice Injector
  
    injector = Guice.createInjector(modules);
    
  Step 5: DEFERRED INJECTION - Now inject dependencies into interceptors
  
    for (interceptor : provider.getInterceptors()) {
        ReferenceInjector.injectReferences(interceptor, injector);
    }
    
    // Now interceptor.userDao, interceptor.someService, etc. are all set!
    
  Step 6: Ready! Dispatcher calls interceptors with fully-injected dependencies
  
    dispatcher.callOperation(...);
    // -> interceptor.preCall() called with working dependencies
    // -> interceptor.postCall() called with working dependencies
```

## Implementation Components

### Component 1: TestOperationCallInterceptorProvider

**Location:** `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/TestOperationCallInterceptorProvider.java`

```java
public class TestOperationCallInterceptorProvider 
        implements OperationCallInterceptorProvider {
    
    private final List<OperationCallInterceptor> interceptors = new ArrayList<>();
    
    /**
     * Add an interceptor to be invoked during operation calls.
     * Can be called before or after init().
     */
    public void addInterceptor(OperationCallInterceptor interceptor) {
        interceptors.add(interceptor);
    }
    
    /**
     * Remove a specific interceptor.
     */
    public void removeInterceptor(OperationCallInterceptor interceptor) {
        interceptors.remove(interceptor);
    }
    
    /**
     * Remove all interceptors.
     */
    public void clearInterceptors() {
        interceptors.clear();
    }
    
    /**
     * Get all registered interceptors (for deferred injection).
     */
    public List<OperationCallInterceptor> getInterceptors() {
        return new ArrayList<>(interceptors);
    }
    
    @Override
    public Collection<OperationCallInterceptor> getCallOperationInterceptors() {
        return new ArrayList<>(interceptors);
    }
}
```

### Component 2: JudoRuntimeFixture Enhancements

**Location:** `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/fixture/JudoRuntimeFixture.java`

**New fields:**
```java
private TestOperationCallInterceptorProvider interceptorProvider;
private List<Class<? extends OperationCallInterceptor>> interceptorClasses = new ArrayList<>();
private List<OperationCallInterceptor> interceptorInstances = new ArrayList<>();
```

**New methods:**
```java
/**
 * Register an interceptor class to be instantiated and injected automatically.
 * Must be called before init().
 * 
 * @param interceptorClass The interceptor class (must have no-arg constructor)
 */
public void addInterceptor(Class<? extends OperationCallInterceptor> interceptorClass) {
    this.interceptorClasses.add(interceptorClass);
}

/**
 * Register a pre-created interceptor instance.
 * Dependencies will be injected after init() is called.
 * Must be called before init().
 * 
 * @param interceptor The interceptor instance
 */
public void addInterceptor(OperationCallInterceptor interceptor) {
    this.interceptorInstances.add(interceptor);
}

/**
 * Get the interceptor provider for advanced manipulation.
 * Available after init() is called.
 * 
 * @return The test interceptor provider
 */
public TestOperationCallInterceptorProvider getInterceptorProvider() {
    return interceptorProvider;
}
```

**Modified init() method:**
```java
public void init(Module module, Object injectModulesTo) {
    // 1. Create interceptor provider
    interceptorProvider = new TestOperationCallInterceptorProvider();
    
    // 2. Instantiate interceptor classes
    for (Class<? extends OperationCallInterceptor> clazz : interceptorClasses) {
        try {
            OperationCallInterceptor instance = clazz.getDeclaredConstructor().newInstance();
            interceptorInstances.add(instance);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate interceptor: " + clazz.getName(), e);
        }
    }
    
    // 3. Register all instances in provider
    for (OperationCallInterceptor interceptor : interceptorInstances) {
        interceptorProvider.addInterceptor(interceptor);
    }
    
    // 4. Build module with custom provider
    judoDefaultModuleBuilder = judoDefaultModuleBuilder
        .injectModulesTo(injectModulesTo)
        .judoModelLoader(modelHolder)
        .extendableCoercer(coercer)
        .queryFactory(queryFactory)
        .operationCallInterceptorProvider(interceptorProvider);  // <-- NEW

    Module modules = Modules.combine(
        module,
        judoDefaultModuleBuilder.build(),
        databaseModule
    );
    
    // 5. Create injector
    injector = Guice.createInjector(modules);
    
    // 6. DEFERRED INJECTION - inject dependencies into interceptors
    for (OperationCallInterceptor interceptor : interceptorInstances) {
        ReferenceInjector.injectReferences(interceptor, injector);
    }
}
```

### Component 3: @JudoTest Annotation Enhancement

**Location:** `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/fixture/JudoTest.java`

**New attribute:**
```java
/**
 * Interceptor classes to register for the test.
 * Each interceptor will be:
 * 1. Instantiated (must have no-arg constructor)
 * 2. Registered in the OperationCallInterceptorProvider
 * 3. Have its @Reference dependencies injected after Guice injector creation
 * 
 * Example:
 * <pre>
 * @JudoTest(interceptors = { OrderCreateInterceptor.class, AuditInterceptor.class })
 * void testWithInterceptors(JudoRuntimeFixture fixture) {
 *     // Interceptors automatically invoked during dispatcher calls
 * }
 * </pre>
 */
Class<? extends OperationCallInterceptor>[] interceptors() default {};
```

### Component 4: JudoTestExtension Enhancement

**Location:** `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/fixture/JudoTestExtension.java`

**Modified beforeEach()** (in the section after creating runtimeFixture):
```java
// Register interceptor classes from annotation
Class<? extends OperationCallInterceptor>[] interceptorClasses = annotation.interceptors();
for (Class<? extends OperationCallInterceptor> clazz : interceptorClasses) {
    runtimeFixture.addInterceptor(clazz);
    log.debug("Registered interceptor class: {}", clazz.getName());
}

// init() will handle instantiation, registration, and injection
runtimeFixture.init(customModule, context.getTestInstance().orElse(null));
```

## Usage Examples

### Example 1: Annotation-Based (Simple)

```java
@JudoTest(
    modelName = "sales",
    interceptors = { OrderCreateInterceptor.class, AuditInterceptor.class }
)
void testOrderCreationWithInterceptors(JudoRuntimeFixture fixture) {
    // Arrange
    Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
    
    // Act - interceptors are automatically invoked!
    Payload result = dispatcher.callOperation(
        OrderService.class,
        "createOrder",
        Payload.map("customerId", 123, "amount", 500.00)
    );
    
    // Assert - verify interceptor side effects
    // e.g., audit log was written, permissions were checked, etc.
}
```

### Example 2: Programmatic (Flexible)

```java
@JudoTest(modelName = "sales")
void testWithCustomInterceptorConfiguration(JudoRuntimeFixture fixture) {
    // Create spy interceptor for verification
    SpyInterceptor spy = new SpyInterceptor();
    fixture.addInterceptor(spy);
    
    // Re-initialize to apply interceptor
    // Note: When using @JudoTest, you'd need to use manual fixture setup instead
    
    Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
    
    // Act
    dispatcher.callOperation(...);
    
    // Assert - inspect spy
    assertThat(spy.getPreCallCount()).isEqualTo(1);
    assertThat(spy.getLastOperation().getName()).isEqualTo("createOrder");
}
```

### Example 3: Manual Fixture Setup with Interceptors

```java
class ManualInterceptorTest {
    
    private DataSource dataSource;
    private JudoRuntimeFixture fixture;
    
    @BeforeEach
    void setUp() throws Exception {
        // Setup datasource
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:test");
        config.setUsername("SA");
        config.setPassword("");
        dataSource = new HikariDataSource(config);
        
        // Create fixture
        fixture = new JudoRuntimeFixture();
        fixture.prepare("sales", dataSource, "hsqldb");
        
        // Register interceptors BEFORE init
        fixture.addInterceptor(OrderCreateInterceptor.class);
        fixture.addInterceptor(new CustomConfiguredInterceptor("config-value"));
        
        // Initialize - interceptors are instantiated and injected
        fixture.init(new AbstractModule() {}, null);
    }
    
    @Test
    void testInterceptorInDispatcherFlow() {
        fixture.beginTransaction();
        try {
            Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
            
            // Interceptors are invoked during this call
            dispatcher.callOperation(...);
            
            fixture.commitTransaction();
        } catch (Exception e) {
            fixture.rollbackTransaction();
            throw e;
        }
    }
    
    @AfterEach
    void tearDown() {
        fixture.tearDown();
        ((HikariDataSource) dataSource).close();
    }
}
```

### Example 4: Unit Testing Interceptor Logic (Already Supported)

```java
@JudoTest(modelName = "sales")
void testInterceptorLogicInIsolation(JudoRuntimeFixture fixture) {
    // Create interceptor with injected dependencies
    OrderCreateInterceptor interceptor = ReferenceInjector.createAndInject(
        OrderCreateInterceptor.class,
        fixture.getInjector()
    );
    
    // Create mock operation and payload
    EOperation operation = fixture.modelHolder.getAsmModel()
        .findOperation("OrderService", "createOrder");
    
    Payload input = Payload.map("customerId", 123);
    
    // Test preCall directly
    fixture.beginTransaction();
    Object result = interceptor.preCall(operation, input);
    fixture.commitTransaction();
    
    // Assert
    assertThat(result).isInstanceOf(Payload.class);
}
```

## Alternative Approaches Considered

### Alternative A: Provider-Based Injection (Lazy)

Use Guice `Provider<T>` for dependencies in interceptors:

```java
class MyInterceptor implements OperationCallInterceptor {
    @Inject
    Provider<UserDao> userDaoProvider;  // Lazy resolution
    
    @Override
    public Object preCall(EOperation op, Object input) {
        UserDao userDao = userDaoProvider.get();  // Resolved at call time
        ...
    }
}
```

**Pros:**
- No timing issues - providers resolve lazily
- Standard Guice pattern

**Cons:**
- Requires changing how interceptors are written
- Not compatible with existing `@Reference` pattern from OSGi
- More boilerplate in interceptor code

**Verdict:** Rejected - changes the programming model and isn't compatible with existing interceptors.

### Alternative B: Two-Phase Initialization

Split `init()` into `initModules()` and `initInterceptors()`:

```java
fixture.prepare(...);
fixture.initModules(module, target);  // Creates injector

// Now injector exists, can create and inject interceptors
MyInterceptor interceptor = ReferenceInjector.createAndInject(
    MyInterceptor.class, fixture.getInjector());
fixture.registerInterceptor(interceptor);

fixture.initDispatcher();  // Creates dispatcher with interceptors
```

**Pros:**
- Explicit control over timing
- Clear separation of concerns

**Cons:**
- Breaking change to API
- More complex for users
- Requires understanding internal architecture

**Verdict:** Possible but too complex for most use cases. The deferred injection approach hides this complexity.

## Implementation Priorities

| Priority | Component | Description | Effort |
|----------|-----------|-------------|--------|
| **P1** | `TestOperationCallInterceptorProvider` | Mutable provider with `add/remove/clear` methods | Small |
| **P1** | `JudoRuntimeFixture` enhancements | `addInterceptor()` methods + deferred injection in `init()` | Medium |
| **P2** | `@JudoTest.interceptors` attribute | Annotation-based interceptor registration | Small |
| **P2** | `JudoTestExtension` enhancements | Handle `interceptors` attribute | Small |
| **P3** | Documentation | Update README with interceptor testing patterns | Small |
| **P3** | Example tests | Real working examples in the examples package | Medium |

## Open Questions

1. **Interceptor lifecycle:**
   - Should interceptors be created per-test or reused across tests?
   - Current proposal: Created per-test (follows fixture lifecycle)

2. **Runtime interceptor modification:**
   - Should the testkit expose the `TestOperationCallInterceptorProvider` to allow adding/removing interceptors mid-test?
   - Current proposal: Yes, via `getInterceptorProvider()` method

3. **Validation:**
   - Should we validate that interceptor classes have no-arg constructors at registration time?
   - Current proposal: Yes, fail fast with clear error message

## Files to Modify/Create

### New Files
- `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/TestOperationCallInterceptorProvider.java`

### Modified Files
- `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/fixture/JudoRuntimeFixture.java`
- `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/fixture/JudoTest.java`
- `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/fixture/JudoTestExtension.java`
- `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/examples/InterceptorIntegrationTest.java` (update examples)
- `judo-runtime-core-guice-testkit/README.md`

## Model-Based Integration Testing

### Overview

For full end-to-end integration testing with actual dispatcher operation calls, you need a generated JUDO model. This section documents how to set up model-based tests.

### Model Files Required

A complete JUDO model consists of several generated files:

| File Pattern | Description |
|--------------|-------------|
| `{modelName}-asm.model` | Application Service Model |
| `{modelName}-rdbms_{dialect}.model` | RDBMS model for the specific dialect |
| `{modelName}-measure.model` | Measurement/metrics model |
| `{modelName}-expression.model` | Expression model |
| `{modelName}-liquibase_{dialect}.changelog.xml` | Database schema changelog |
| `{modelName}-asm2rdbms_{dialect}.model` | ASM to RDBMS transformation trace |

### Model Generation

Models are generated from JSL (JUDO Script Language) files using the judo-tatami transformation pipeline:

```
  JSL File                judo-tatami               Generated Models
 +----------------+      +-----------+      +------------------------+
 | MyModel.jsl    |----->| Transform |----->| MyModel-asm.model      |
 |                |      | Pipeline  |      | MyModel-rdbms_*.model  |
 | model MyModel; |      |           |      | MyModel-measure.model  |
 | entity User {} |      |           |      | ...                    |
 +----------------+      +-----------+      +------------------------+
```

Example JSL model (from `judo-tatami-jsl-tests/models/ActionGroupTest`):

```jsl
model ActionGroupTest;

// Types
type string String min-size: 0 max-size: 255;
type boolean Boolean;

// Entity
entity Galaxy {
    field String name required;
    relation Matter[] matter;
}

// Transfer
transfer GalaxyTransfer(Galaxy galaxy) {
    field String name <=> galaxy.name;
    action void createInterstellarMedium(MatterCreator matterCreator);
}

// Actor
actor God {
    access GalaxyTransfer[] galaxies <= Galaxy.all() create delete update;
}
```

### Loading Models

#### From Filesystem (Development)

```java
File modelDir = new File("target/generated-test-sources/model");
JudoModelLoader modelLoader = JudoModelLoader.loadFromDirectory(
    "ActionGroupTest",       // modelName
    modelDir,                // directory with model files
    new HsqldbDialect(),     // database dialect
    false                    // loadKeycloak
);
```

#### From Classpath (Packaged Tests)

```java
JudoModelLoader modelLoader = JudoModelLoader.loadFromClassloader(
    "ActionGroupTest",              // modelName
    getClass().getClassLoader(),    // classLoader
    new HsqldbDialect(),            // dialect
    true,                           // validate
    false                           // loadKeycloak
);
```

### Complete Model-Based Test Example

```java
@JudoTest(
    modelName = "ActionGroupTest",
    modelSource = JudoTest.ModelSource.CLASSPATH,
    dialect = "hsqldb",
    transaction = JudoTest.TransactionHandling.AUTO_ROLLBACK,
    interceptors = { AuditInterceptor.class }
)
void testInterceptorWithRealDispatcherOperations(JudoRuntimeFixture fixture) {
    // Get dispatcher
    Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
    
    // Get interceptor for verification
    AuditInterceptor audit = (AuditInterceptor) fixture
        .getInterceptorProvider().getInterceptors().get(0);
    
    int initialCount = audit.getCallCount();
    
    // Call a REAL operation through the dispatcher
    // This triggers the full interceptor flow: preCall -> operation -> postCall
    Map<String, Object> result = dispatcher.callOperation(
        "ActionGroupTest.GalaxyTransfer.createInterstellarMedium",  // operation FQN
        Payload.map(
            "__exposed", true,              // Required for public operations
            "matterCreator", Payload.map(
                "mass", 1000.00,
                "shortNote", "testmatter"
            )
        )
    );
    
    // Verify operation completed
    assertNotNull(result);
    
    // Verify interceptor was invoked
    assertEquals(initialCount + 1, audit.getCallCount());
    assertEquals("createInterstellarMedium", audit.getLastOperationName());
}
```

### Key Considerations

1. **Models are NOT in Git**: Generated model files are typically not committed to version control. They're generated during the build process.

2. **Build Order**: When setting up CI/CD, ensure model generation runs before tests that depend on those models.

3. **Operation FQN Format**: `{modelName}.{TransferName}.{operationName}`

4. **Exposed Operations**: Include `"__exposed": true` in the payload for operations exposed to actors.

5. **Test Isolation**: Use `AUTO_ROLLBACK` transaction handling to keep tests isolated.

### Testing Without a Real Model

For many interceptor testing scenarios, you don't need a full model:

- **Unit Testing**: Test `preCall()`/`postCall()` logic directly
- **Framework Testing**: Verify interceptor registration/injection works
- **Mock-Based Testing**: Mock the operation and verify interceptor behavior

The `JudoModelLoader.empty()` method provides a minimal model sufficient for these scenarios.
