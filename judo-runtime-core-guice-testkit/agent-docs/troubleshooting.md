# Troubleshooting

## Common Errors

### IllegalStateException: Fixture already initialized

**Error:**
```
java.lang.IllegalStateException: Cannot add interceptor after fixture has been initialized
```

**Cause:** Called `addInterceptor()` after `init()` was called.

**Fix:** Move `addInterceptor()` before `init()`:
```java
fixture.prepare(...);
fixture.addInterceptor(MyInterceptor.class);  // BEFORE init()
fixture.init(...);
```

---

### IllegalStateException: Fixture not initialized

**Error:**
```
java.lang.IllegalStateException: Cannot get interceptor provider before fixture is initialized
```

**Cause:** Called `getInterceptorProvider()` before `init()` was called.

**Fix:** Call `getInterceptorProvider()` after `init()`:
```java
fixture.prepare(...);
fixture.init(...);
TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();  // AFTER init()
```

---

### NoSuchMethodException: No no-arg constructor

**Error:**
```
java.lang.NoSuchMethodException: MyInterceptor.<init>()
```

**Cause:** Interceptor class doesn't have a no-argument constructor.

**Fix:** Add no-arg constructor:
```java
public class MyInterceptor implements OperationCallInterceptor {
    
    public MyInterceptor() {  // Required for testkit
    }
    
    // Or use instance registration instead:
    // fixture.addInterceptor(new MyInterceptor(param1, param2));
}
```

---

### NullPointerException in interceptor

**Error:**
```
java.lang.NullPointerException
    at MyInterceptor.preCall(MyInterceptor.java:25)
```

**Cause:** Dependencies not injected. Usually happens when:
1. Not using `@JudoTest` annotation
2. Creating interceptor manually without injection
3. Calling `init()` was forgotten

**Fix Options:**

Option 1 - Use `@JudoTest`:
```java
@JudoTest(interceptors = {MyInterceptor.class})  // Auto-injects deps
void test(JudoRuntimeFixture fixture) { ... }
```

Option 2 - Use `ReferenceInjector`:
```java
MyInterceptor interceptor = ReferenceInjector.createAndInject(
    MyInterceptor.class, fixture.getInjector());
```

Option 3 - Ensure `init()` called:
```java
fixture.prepare(...);
fixture.addInterceptor(MyInterceptor.class);
fixture.init(...);  // This triggers dependency injection
```

---

### IllegalArgumentException: Interceptor cannot be null

**Error:**
```
java.lang.IllegalArgumentException: Interceptor cannot be null
```

**Cause:** Passed `null` to `addInterceptor()` or provider's `addInterceptor()`.

**Fix:** Ensure interceptor is not null:
```java
OperationCallInterceptor interceptor = createInterceptor();
if (interceptor != null) {
    fixture.addInterceptor(interceptor);
}
```

---

### Model not found

**Error:**
```
java.lang.IllegalArgumentException: Could not load model from classpath: myModel
```

**Cause:** Model files not in expected location.

**Fix:** Check model location based on `modelSource`:

| ModelSource | Expected Location |
|-------------|-------------------|
| `FILESYSTEM` | `target/generated-test-sources/model/{modelName}-asm.model` |
| `CLASSPATH` | JAR's `/model/{modelName}-asm.model` |
| `AUTO` | Tries filesystem first, then classpath |

For development, ensure model is generated:
```bash
mvn generate-test-sources
```

---

### Transaction already active

**Error:**
```
java.lang.IllegalStateException: Transaction already active
```

**Cause:** Called `beginTransaction()` when transaction already started.

**Fix Options:**

Option 1 - Use `@JudoTest` with `AUTO_ROLLBACK` (handles transactions):
```java
@JudoTest(transaction = TransactionHandling.AUTO_ROLLBACK)
void test(JudoRuntimeFixture fixture) {
    // Transaction auto-managed, don't call beginTransaction()
}
```

Option 2 - Use `MANUAL` and manage yourself:
```java
@JudoTest(transaction = TransactionHandling.MANUAL)
void test(JudoRuntimeFixture fixture) {
    fixture.beginTransaction();
    try {
        // test code
        fixture.commitTransaction();
    } catch (Exception e) {
        fixture.rollbackTransaction();
        throw e;
    }
}
```

---

### Test methods see stale interceptor state under BY_CLASS / SINGLETON

**Symptom:** A test passes in isolation but fails when run as part of a
`BY_CLASS` or `SINGLETON` test suite, with assertions like
`expected callCount=1 but was 4` or counters that keep growing across methods.

**Cause:** `@JudoTest(dataSourceMode = BY_CLASS | SINGLETON)` caches the Guice
`Injector` and therefore the same interceptor instances across every method of
the class (and across every class for SINGLETON). If your interceptor
accumulates state (counters, captured calls, mutable lists, ...) it will leak
between methods.

**Fix Options:**

Option 1 — Reset state in `@BeforeEach`:
```java
@BeforeEach
void resetInterceptor(JudoRuntimeFixture fixture) {
    MyCountingInterceptor i = fixture.getInjector().getInstance(MyCountingInterceptor.class);
    i.reset();
}
```

Option 2 — Pin the test class to BY_METHOD:
```java
@JudoTest(dataSourceMode = JudoTest.DataSourceMode.BY_METHOD,
          interceptors = { MyCountingInterceptor.class })
class MyTest { ... }
```

---

### Schema changes from one test method are visible in the next

**Symptom:** A test that drops/recreates a table or alters a column passes,
but subsequent methods of the same class fail with `table not found` or
schema mismatch errors.

**Cause:** Under `BY_CLASS` and `SINGLETON`, Liquibase runs **exactly once**
per cached runtime, NOT once per method. If a test method mutates the schema,
those mutations persist for the rest of the class and are NOT reverted by a
fresh Liquibase migration on the next method.

**Fix:** Switch the schema-mutating test class to `BY_METHOD`:
```java
@JudoTest(dataSourceMode = JudoTest.DataSourceMode.BY_METHOD)
class SchemaMutatingTest { ... }
```

This re-runs Liquibase per method, restoring a clean schema.

---

### Subclass override of `JudoRuntimeFixture#init(…)` is not invoked

**Symptom:** Custom subclass of `JudoRuntimeFixture` overrides `init(…)` to
do extra setup, but the override never fires.

**Cause:** On the cached `BY_CLASS` / `SINGLETON` code path, `JudoTestExtension`
uses `prepareWithCachedRuntime(…)` instead of `init(…)` after the first
method — by design, since the artifacts are pre-built. Subclass `init`
overrides are bypassed.

**Fix:** Use `BY_METHOD` for tests that depend on a custom `init` override:
```java
@JudoTest(dataSourceMode = JudoTest.DataSourceMode.BY_METHOD)
class MyCustomFixtureTest { ... }
```

---

## Diagnostic Checklist

When interceptor tests fail, check:

- [ ] Interceptor has no-arg constructor
- [ ] `addInterceptor()` called BEFORE `init()`
- [ ] `getInterceptorProvider()` called AFTER `init()`
- [ ] Using `@JudoTest` or calling `init()` manually
- [ ] Dependencies use `@Reference` or `@Inject` annotation
- [ ] Transaction handling matches test needs
- [ ] Model files exist (for model-based tests)

## Debug Logging

Enable debug logging to see interceptor registration:

```xml
<!-- logback-test.xml -->
<logger name="hu.blackbelt.judo.runtime.core.guice.testkit" level="DEBUG"/>
```

Log output shows:
```
DEBUG - Registered interceptor class from annotation: MyInterceptor
DEBUG - Instantiated interceptor: MyInterceptor
DEBUG - Injected dependencies into interceptor: MyInterceptor
```

## Getting Help

1. Check full documentation: `README.md` in JAR root
2. Check test configuration: `TEST-CONFIGURATION.md` in JAR root  
3. See example tests: `InterceptorIntegrationTest.java` in testkit source
