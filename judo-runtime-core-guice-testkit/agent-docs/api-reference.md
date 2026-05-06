# API Reference

## JudoRuntimeFixture

Main test fixture for JUDO runtime testing.

**Package:** `hu.blackbelt.judo.runtime.core.guice.testkit.fixture`

### Lifecycle Methods

| Method | Description |
|--------|-------------|
| `prepare(modelName, dataSource, dialect)` | Initialize with model name and datasource |
| `prepare(JudoModelLoader, dataSource, dialect)` | Initialize with pre-loaded model |
| `init(Module, injectTarget)` | Create Guice injector and initialize runtime |
| `tearDown()` | Clean up resources (does NOT close the injector or datasource on cached scopes) |

#### Cached fast path (BY_CLASS / SINGLETON)

`JudoTestExtension` uses an internal fast path on cached scopes that bypasses
`prepare(…)` and `init(…)` after the first method:

| Method (package-private) | Description |
|--------------------------|-------------|
| `prepareWithCachedRuntime(CachedRuntime, Object)` | Installs the cached `Injector`, `QueryFactory`, `PlatformTransactionManager`, database `Module`, Liquibase executor, model loader, and dialect. Optionally runs `injector.injectMembers(testInstance)`. |

`CachedRuntime` is an immutable bundle of the derived runtime artifacts plus an
idempotent `close()` (it implements `ExtensionContext.Store.CloseableResource`).
Its lifecycle is:

- **BY_CLASS**: built on the first method; stored in JUnit's class-scoped
  `Store`; closed by JUnit when the class store is cleaned up.
- **SINGLETON**: built on first access in any test class with a matching
  configuration; stored in a JVM-wide `ConcurrentHashMap` keyed by
  `(modelName, dialect, modelSource, modules, interceptors)`; all entries are
  closed exactly once at JVM shutdown via a single root-store
  `CloseableResource`.

User code does NOT call `prepareWithCachedRuntime` directly — it is invoked
by `JudoTestExtension` on cached scopes. The public API surface
(`@JudoTest`, `DataSourceMode`) is unchanged.

### Interceptor Methods

| Method | Description | Timing |
|--------|-------------|--------|
| `addInterceptor(Class<? extends OperationCallInterceptor>)` | Register interceptor by class | Before `init()` |
| `addInterceptor(OperationCallInterceptor)` | Register interceptor instance | Before `init()` |
| `getInterceptorProvider()` | Get mutable interceptor provider | After `init()` |

### Transaction Methods

| Method | Description |
|--------|-------------|
| `beginTransaction()` | Start new transaction |
| `commitTransaction()` | Commit current transaction |
| `rollbackTransaction()` | Rollback current transaction |
| `createSavePoint()` | Create savepoint for partial rollback |
| `rollbackToSavePoint(savepoint)` | Rollback to savepoint |

### Accessor Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `getInjector()` | `Injector` | Guice injector for getting instances |
| `getInterceptorProvider()` | `TestOperationCallInterceptorProvider` | Interceptor provider |

### Example

```java
JudoRuntimeFixture fixture = new JudoRuntimeFixture();
fixture.prepare("myModel", dataSource, "hsqldb");
fixture.addInterceptor(MyInterceptor.class);
fixture.init(new AbstractModule() {}, null);

Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
```

---

## @JudoTest Annotation

Declarative test configuration annotation.

**Package:** `hu.blackbelt.judo.runtime.core.guice.testkit.fixture`

### Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `modelName` | `String` | `"exa"` | JUDO model name |
| `dialect` | `String` | `"hsqldb"` | Database dialect |
| `modelSource` | `ModelSource` | `AUTO` | Where to load model from |
| `transaction` | `TransactionHandling` | `AUTO_ROLLBACK` | Transaction mode |
| `interceptors` | `Class[]` | `{}` | Interceptor classes to register |
| `modules` | `Class[]` | `{}` | Custom Guice modules |

### ModelSource Enum

| Value | Description |
|-------|-------------|
| `AUTO` | Try filesystem first, then classpath |
| `FILESYSTEM` | Load from `target/generated-test-sources/model/` |
| `CLASSPATH` | Load from JAR classpath |

### TransactionHandling Enum

| Value | Description |
|-------|-------------|
| `AUTO_ROLLBACK` | Auto begin, auto rollback after test |
| `AUTO_COMMIT` | Auto begin, auto commit, truncate tables |
| `MANUAL` | You call begin/commit/rollback |
| `NONE` | No transaction management |

### Example

```java
@JudoTest(
    modelName = "sales",
    dialect = "postgresql",
    transaction = TransactionHandling.AUTO_ROLLBACK,
    interceptors = {AuditInterceptor.class, ValidationInterceptor.class}
)
void testWithAnnotation(JudoRuntimeFixture fixture) {
    // fixture is auto-configured and injected
}
```

---

## ReferenceInjector

Utility for injecting dependencies into objects.

**Package:** `hu.blackbelt.judo.runtime.core.guice.testkit.util`

### Static Methods

| Method | Description |
|--------|-------------|
| `createAndInject(Class<T>, Injector)` | Create instance and inject all dependencies |
| `injectReferences(Object, Injector)` | Inject dependencies into existing instance |

### What Gets Injected

- All non-static, non-final fields
- Setter methods named `set*`
- Fields/setters with `@Reference` annotation (OSGi)
- Fields/setters with `@Inject` annotation (Guice)

### Example

```java
// Create new instance with injected deps
MyInterceptor interceptor = ReferenceInjector.createAndInject(
    MyInterceptor.class, 
    fixture.getInjector()
);

// Inject into existing instance
MyInterceptor existing = new MyInterceptor();
ReferenceInjector.injectReferences(existing, fixture.getInjector());
```

---

## TestOperationCallInterceptorProvider

Mutable interceptor provider for test scenarios.

**Package:** `hu.blackbelt.judo.runtime.core.guice.testkit.util`

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `addInterceptor(OperationCallInterceptor)` | `void` | Add interceptor |
| `removeInterceptor(OperationCallInterceptor)` | `boolean` | Remove specific interceptor |
| `clearInterceptors()` | `void` | Remove all interceptors |
| `getInterceptors()` | `List<OperationCallInterceptor>` | Get defensive copy of interceptors |
| `size()` | `int` | Number of registered interceptors |
| `isEmpty()` | `boolean` | True if no interceptors registered |

### Example

```java
TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();

// Add at runtime
provider.addInterceptor(new SpyInterceptor());

// Check state
assertEquals(1, provider.size());

// Get for verification
SpyInterceptor spy = (SpyInterceptor) provider.getInterceptors().get(0);

// Clear all
provider.clearInterceptors();
assertTrue(provider.isEmpty());
```

---

## JudoModelLoader

Load JUDO models for testing.

**Package:** `hu.blackbelt.judo.runtime.core.guice`

### Static Factory Methods

| Method | Description |
|--------|-------------|
| `empty()` | Create minimal empty model (for framework tests) |
| `loadFromDirectory(name, dir, dialect, loadKeycloak)` | Load from filesystem |
| `loadFromClassloader(name, classLoader, dialect, validate, loadKeycloak)` | Load from JAR |

### Example

```java
// Empty model (no operations, just framework testing)
JudoModelLoader loader = JudoModelLoader.empty();

// From filesystem (development)
JudoModelLoader loader = JudoModelLoader.loadFromDirectory(
    "ActionGroupTest",
    new File("target/generated-test-sources/model"),
    new HsqldbDialect(),
    false  // loadKeycloak
);

// From classpath (packaged tests)
JudoModelLoader loader = JudoModelLoader.loadFromClassloader(
    "ActionGroupTest",
    getClass().getClassLoader(),
    new HsqldbDialect(),
    true,   // validate
    false   // loadKeycloak
);

// Use with fixture
fixture.prepare(loader, dataSource, "hsqldb");
```

---

## OperationCallInterceptor Interface

Interface for intercepting dispatcher operations.

**Package:** `hu.blackbelt.judo.runtime.core.dispatcher`

### Methods

| Method | Default | Description |
|--------|---------|-------------|
| `getName()` | (required) | Unique interceptor name |
| `getOperations(AsmModel)` | `emptyList()` | Operations to intercept (empty = all) |
| `preCall(EOperation, Object)` | return input | Called before operation |
| `postCall(EOperation, Object, Object)` | return output | Called after operation |

### Example Implementation

```java
public class AuditInterceptor implements OperationCallInterceptor {
    
    @Reference  // Injected by ReferenceInjector
    private AuditService auditService;
    
    @Override
    public String getName() {
        return "AuditInterceptor";
    }
    
    @Override
    public Collection<EOperation> getOperations(AsmModel asmModel) {
        return Collections.emptyList();  // Intercept all
    }
    
    @Override
    public Object preCall(EOperation operation, Object payload) {
        auditService.logOperationStart(operation.getName());
        return payload;  // Pass through unchanged
    }
    
    @Override
    public Object postCall(EOperation op, Object input, Object output) {
        auditService.logOperationEnd(op.getName());
        return output;  // Pass through unchanged
    }
}
```
