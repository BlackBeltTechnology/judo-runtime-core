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
