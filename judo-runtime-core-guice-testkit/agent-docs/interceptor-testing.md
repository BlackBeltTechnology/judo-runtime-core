# Interceptor Testing - Deep Dive

## OperationCallInterceptor Interface

```java
public interface OperationCallInterceptor {
    String getName();
    
    // Return empty = intercept ALL operations
    default Collection<EOperation> getOperations(AsmModel asmModel) {
        return Collections.emptyList();
    }
    
    // Called BEFORE operation execution
    default Object preCall(EOperation operation, Object parameterPayload) {
        return parameterPayload;  // Can modify input
    }
    
    // Called AFTER operation execution  
    default Object postCall(EOperation operation, Object parameterPayload, Object returnPayload) {
        return returnPayload;  // Can modify output
    }
}
```

## Interceptor Execution Flow

```
Dispatcher.callOperation()
    │
    ▼
Validate operation exists
    │
    ▼
FOR EACH interceptor (in order):
    interceptor.preCall(operation, input)
    │
    ▼
Execute actual operation
    │
    ▼
FOR EACH interceptor (REVERSE order):
    interceptor.postCall(operation, input, output)
    │
    ▼
Return result
```

## The Timing Problem (Why Deferred Injection)

```
PROBLEM:
┌─────────────────────────────────────────────────────────────────┐
│ Interceptors need dependencies (DAOs, services)                 │
│ Dependencies come from Guice Injector                          │
│ BUT Injector is created AFTER InterceptorProvider is configured │
│ = Chicken-and-egg problem                                       │
└─────────────────────────────────────────────────────────────────┘

SOLUTION (Deferred Injection):
1. Create interceptor instances (deps are null)
2. Register in TestOperationCallInterceptorProvider
3. Build Guice modules with custom provider
4. Create Injector
5. NOW inject dependencies into interceptors
6. Ready - interceptors have working dependencies
```

## Testing Patterns

### 1. Annotation-Based (Recommended for Integration Tests)

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import static org.junit.jupiter.api.Assertions.*;

class MyInterceptorIntegrationTest {

    @JudoTest(interceptors = {AuditInterceptor.class, ValidationInterceptor.class})
    void testMultipleInterceptors(JudoRuntimeFixture fixture) {
        Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
        
        // Both interceptors invoked in order for preCall
        // Both interceptors invoked in REVERSE order for postCall
        Map<String, Object> result = dispatcher.callOperation(
            "model.User.createUser",
            Payload.map("__exposed", true, "email", "test@example.com")
        );
        
        assertNotNull(result);
    }
}
```

### 2. Programmatic (For Dynamic Configuration)

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.TestOperationCallInterceptorProvider;
import com.google.inject.AbstractModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

class ProgrammaticInterceptorTest {

    private JudoRuntimeFixture fixture;
    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        // 1. Create datasource
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:test");
        config.setUsername("SA");
        config.setPassword("");
        dataSource = new HikariDataSource(config);
        
        // 2. Create and prepare fixture
        fixture = new JudoRuntimeFixture();
        fixture.prepare("modelName", dataSource, "hsqldb");
        
        // 3. Register interceptors BEFORE init()
        fixture.addInterceptor(AuditInterceptor.class);
        fixture.addInterceptor(new ConfiguredInterceptor("custom-value"));
        
        // 4. Initialize - triggers deferred injection
        fixture.init(new AbstractModule() {}, null);
    }

    @AfterEach
    void tearDown() {
        fixture.tearDown();
        ((HikariDataSource) dataSource).close();
    }

    @Test
    void testInterceptor() {
        fixture.beginTransaction();
        try {
            // Test code here
            fixture.commitTransaction();
        } catch (Exception e) {
            fixture.rollbackTransaction();
            throw e;
        }
    }
}
```

### 3. Unit Testing (Isolated Logic)

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.ReferenceInjector;

class InterceptorUnitTest {

    @JudoTest
    void testPreCallLogic(JudoRuntimeFixture fixture) {
        // Create interceptor with injected dependencies
        MyInterceptor interceptor = ReferenceInjector.createAndInject(
            MyInterceptor.class, fixture.getInjector());
        
        // Test preCall directly
        Payload input = Payload.map("email", "test@example.com");
        Object result = interceptor.preCall(null, input);
        
        // Assert
        assertNotNull(result);
        assertTrue(result instanceof Payload);
    }

    @JudoTest
    void testPostCallLogic(JudoRuntimeFixture fixture) {
        MyInterceptor interceptor = ReferenceInjector.createAndInject(
            MyInterceptor.class, fixture.getInjector());
        
        Payload input = Payload.map("email", "test@example.com");
        Payload output = Payload.map("__identifier", 123L);
        
        Object result = interceptor.postCall(null, input, output);
        
        assertNotNull(result);
    }
}
```

### 4. Spy Interceptor Pattern

```java
public class SpyInterceptor implements OperationCallInterceptor {
    private final AtomicInteger preCallCount = new AtomicInteger(0);
    private final AtomicInteger postCallCount = new AtomicInteger(0);
    private EOperation lastOperation;

    @Override
    public String getName() { return "SpyInterceptor"; }

    @Override
    public Object preCall(EOperation operation, Object payload) {
        preCallCount.incrementAndGet();
        lastOperation = operation;
        return payload;
    }

    @Override
    public Object postCall(EOperation op, Object input, Object output) {
        postCallCount.incrementAndGet();
        return output;
    }

    // Getters for verification
    public int getPreCallCount() { return preCallCount.get(); }
    public int getPostCallCount() { return postCallCount.get(); }
    public EOperation getLastOperation() { return lastOperation; }
    public void reset() { 
        preCallCount.set(0); 
        postCallCount.set(0); 
        lastOperation = null; 
    }
}

// Usage:
@JudoTest(interceptors = {SpyInterceptor.class})
void testWithSpy(JudoRuntimeFixture fixture) {
    SpyInterceptor spy = (SpyInterceptor) fixture
        .getInterceptorProvider().getInterceptors().get(0);
    
    dispatcher.callOperation(...);
    
    assertEquals(1, spy.getPreCallCount());
    assertEquals(1, spy.getPostCallCount());
}
```

> ⚠️ **Stateful interceptors under `BY_CLASS` / `SINGLETON`**
>
> The cached runtime path reuses the same Guice `Injector`, and therefore the
> same interceptor *instance*, across every test method of the class (or
> across every class for `SINGLETON`). Counter fields like `preCallCount`
> above will accumulate across methods. Three options:
>
> 1. Call `spy.reset()` in `@BeforeEach` (cheapest — keeps the cached
>    injector, just resets the spy's fields).
> 2. Disable the runtime cache for this class only, keeping the per-class
>    DataSource for performance:
>    ```java
>    @JudoTest(dataSourceMode = JudoTest.DataSourceMode.BY_CLASS,
>              cacheRuntime = false,
>              interceptors = { SpyInterceptor.class })
>    class SpyTest { ... }
>    ```
>    Every method gets a fresh `SpyInterceptor` instance; the DataSource
>    is still shared per class.
> 3. Drop to `BY_METHOD` (most isolated, slowest — every method also
>    rebuilds the DataSource and re-runs Liquibase):
>    ```java
>    @JudoTest(dataSourceMode = JudoTest.DataSourceMode.BY_METHOD,
>              interceptors = { SpyInterceptor.class })
>    class SpyTest { ... }
>    ```

### 5. Mid-Test Interceptor Manipulation

```java
@JudoTest(interceptors = {InterceptorA.class})
void testDynamicInterceptors(JudoRuntimeFixture fixture) {
    TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();
    
    // Initial state
    assertEquals(1, provider.size());
    
    // Add another interceptor at runtime
    provider.addInterceptor(new InterceptorB());
    assertEquals(2, provider.size());
    
    // Call operation - both interceptors invoked
    dispatcher.callOperation(...);
    
    // Remove all interceptors
    provider.clearInterceptors();
    assertTrue(provider.isEmpty());
    
    // Call operation - no interceptors invoked
    dispatcher.callOperation(...);
}
```

## Model-Based Integration Testing

For testing with real dispatcher operations through a JUDO model:

```java
@JudoTest(
    modelName = "ActionGroupTest",
    modelSource = JudoTest.ModelSource.CLASSPATH,
    dialect = "hsqldb",
    interceptors = {AuditInterceptor.class}
)
void testWithRealModel(JudoRuntimeFixture fixture) {
    Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
    
    // Call real operation - interceptor invoked
    Map<String, Object> result = dispatcher.callOperation(
        "ActionGroupTest.GalaxyTransfer.createInterstellarMedium",
        Payload.map(
            "__exposed", true,
            "matterCreator", Payload.map(
                "mass", 1000.00,
                "shortNote", "test"
            )
        )
    );
}
```

**Note:** Model-based tests require generated model files. Models are generated 
from JSL files during build, not committed to git. See `docs/interceptor_implementation_plan.md`
for details on model generation.

## Checklist: Writing Interceptor Tests

- [ ] Interceptor has no-arg constructor
- [ ] `addInterceptor()` called before `init()` (programmatic)
- [ ] `getInterceptorProvider()` called after `init()`
- [ ] Using `@JudoTest` or manual transaction management
- [ ] Dependencies use `@Reference` annotation (field or setter)
