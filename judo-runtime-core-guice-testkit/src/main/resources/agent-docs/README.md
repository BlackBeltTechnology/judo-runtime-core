# judo-runtime-core-guice-testkit - Agent Documentation

Test toolkit for JUDO runtime components outside OSGi. Provides dependency 
injection and test fixtures for interceptors, operations, and custom components.

## When to Use What

```
Testing interceptor?
├─► Unit test (test preCall/postCall directly)
│   └─► ReferenceInjector.createAndInject(MyInterceptor.class, injector)
│
├─► Integration test (interceptor invoked by dispatcher)
│   ├─► Annotation-based (recommended)
│   │   └─► @JudoTest(interceptors = {MyInterceptor.class})
│   └─► Programmatic  
│       └─► fixture.addInterceptor(MyInterceptor.class) // before init()
│
└─► Model-based test (with real JUDO model)
    └─► JudoModelLoader.loadFromDirectory(name, dir, dialect, false)
```

## Key Classes

| Class | Purpose | Key Methods |
|-------|---------|-------------|
| `JudoRuntimeFixture` | Main test fixture | `prepare()`, `init()`, `addInterceptor()`, `getInterceptorProvider()` |
| `@JudoTest` | Annotation for declarative tests | `interceptors`, `transaction`, `modelName`, `dialect` |
| `ReferenceInjector` | Inject deps into objects | `createAndInject()`, `injectReferences()` |
| `TestOperationCallInterceptorProvider` | Mutable interceptor provider | `addInterceptor()`, `removeInterceptor()`, `clearInterceptors()` |
| `JudoModelLoader` | Load JUDO models | `loadFromDirectory()`, `loadFromClassloader()`, `empty()` |

## Quick Patterns

### Pattern 1: Annotation-Based Interceptor Test (Simplest)

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;

@JudoTest(interceptors = {MyInterceptor.class})
void testInterceptorInDispatcherFlow(JudoRuntimeFixture fixture) {
    Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
    // MyInterceptor.preCall() and postCall() invoked automatically
    dispatcher.callOperation("model.Entity.operation", payload);
}
```

### Pattern 2: Programmatic Interceptor Registration

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import com.google.inject.AbstractModule;

// In @BeforeEach or test setup:
JudoRuntimeFixture fixture = new JudoRuntimeFixture();
fixture.prepare("modelName", dataSource, "hsqldb");
fixture.addInterceptor(MyInterceptor.class);  // MUST be before init()
fixture.init(new AbstractModule() {}, null);
```

### Pattern 3: Unit Test Interceptor Logic

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.ReferenceInjector;

@JudoTest
void testInterceptorLogicDirectly(JudoRuntimeFixture fixture) {
    MyInterceptor interceptor = ReferenceInjector.createAndInject(
        MyInterceptor.class, fixture.getInjector());
    
    Object result = interceptor.preCall(operation, inputPayload);
    // Assert on result
}
```

### Pattern 4: Access Interceptor Provider Mid-Test

```java
@JudoTest(interceptors = {SpyInterceptor.class})
void testWithProviderAccess(JudoRuntimeFixture fixture) {
    TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();
    
    // Get registered interceptor
    SpyInterceptor spy = (SpyInterceptor) provider.getInterceptors().get(0);
    
    // Call operation
    dispatcher.callOperation(...);
    
    // Verify interceptor was invoked
    assertEquals(1, spy.getCallCount());
    
    // Clear interceptors mid-test if needed
    provider.clearInterceptors();
}
```

## Constraints

| Constraint | Details |
|------------|---------|
| `addInterceptor()` timing | MUST call before `init()` |
| Interceptor constructor | MUST have no-arg constructor |
| `getInterceptorProvider()` timing | MUST call after `init()` |
| Transaction in @JudoTest | Default: `AUTO_ROLLBACK` |
| Model files | Generated during build, not in git |

## Transaction Handling

| Mode | Behavior |
|------|----------|
| `AUTO_ROLLBACK` (default) | Transaction started, rolled back after test |
| `AUTO_COMMIT` | Transaction committed, tables truncated after |
| `MANUAL` | You control `beginTransaction()`, `commitTransaction()`, `rollbackTransaction()` |
| `NONE` | No transaction management |

## Files in This Package

| File | Content |
|------|---------|
| `README.md` | This file - overview and quick patterns |
| `interceptor-testing.md` | Deep dive on interceptor testing patterns |
| `api-reference.md` | Key classes and methods reference |
| `troubleshooting.md` | Common errors and solutions |
| `TEST-CONFIGURATION.md` | Test setup, dependencies, build order |

## See Also

- Full documentation: `README.md` in JAR root (62KB cookbook)
- Source: `hu.blackbelt.judo.runtime.core.guice.testkit` package
