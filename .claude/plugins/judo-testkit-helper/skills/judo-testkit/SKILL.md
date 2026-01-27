---
name: judo-testkit
description: Help write tests for JUDO applications using judo-runtime-core-guice-testkit. Use when writing interceptor tests, setting up JudoRuntimeFixture, or troubleshooting testkit issues.
license: EPL-2.0
compatibility: Requires judo-runtime-core-guice-testkit dependency.
metadata:
  author: BlackBelt Technology
  version: "1.0.0"
---

Help write tests for JUDO applications using judo-runtime-core-guice-testkit.

## When to Use

Use this skill when:
- Writing tests for OperationCallInterceptor implementations
- Setting up JudoRuntimeFixture for integration tests
- Using @JudoTest annotation
- Troubleshooting testkit issues

## Context

Read the agent documentation from the testkit JAR for detailed patterns:

**Location:** `~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-guice-testkit/{version}/judo-runtime-core-guice-testkit-{version}.jar!/agent-docs/`

**Files:**
- `README.md` - Quick patterns and decision tree
- `interceptor-testing.md` - Full interceptor testing guide  
- `api-reference.md` - Key classes reference
- `troubleshooting.md` - Common errors and fixes
- `TEST-CONFIGURATION.md` - Test setup and dependencies

---

## Quick Reference

### Testing Decision Tree

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

### Key Classes

| Class | Purpose |
|-------|---------|
| `JudoRuntimeFixture` | Main test fixture |
| `@JudoTest` | Annotation for declarative tests |
| `ReferenceInjector` | Inject deps into objects |
| `TestOperationCallInterceptorProvider` | Mutable interceptor provider |
| `JudoModelLoader` | Load JUDO models |

---

## Patterns

### Pattern 1: Annotation-Based (Simplest)

```java
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;

@JudoTest(interceptors = {MyInterceptor.class})
void testInterceptor(JudoRuntimeFixture fixture) {
    Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
    // MyInterceptor automatically registered and invoked
    dispatcher.callOperation("model.Entity.operation", payload);
}
```

### Pattern 2: Programmatic

```java
JudoRuntimeFixture fixture = new JudoRuntimeFixture();
fixture.prepare("modelName", dataSource, "hsqldb");
fixture.addInterceptor(MyInterceptor.class);  // MUST be before init()
fixture.init(new AbstractModule() {}, null);
```

### Pattern 3: Unit Test

```java
@JudoTest
void testLogic(JudoRuntimeFixture fixture) {
    MyInterceptor interceptor = ReferenceInjector.createAndInject(
        MyInterceptor.class, fixture.getInjector());
    
    Object result = interceptor.preCall(operation, payload);
    // Assert directly
}
```

---

## Constraints

| Constraint | Details |
|------------|---------|
| `addInterceptor()` | MUST call before `init()` |
| Interceptor constructor | MUST have no-arg constructor |
| `getInterceptorProvider()` | MUST call after `init()` |
| Transaction default | `AUTO_ROLLBACK` |

---

## Common Errors

| Error | Fix |
|-------|-----|
| `IllegalStateException: already initialized` | Move `addInterceptor()` before `init()` |
| `NoSuchMethodException` | Add no-arg constructor to interceptor |
| `NullPointerException` in interceptor | Ensure using `@JudoTest` or calling `init()` |

---

## Dependencies

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-guice-testkit</artifactId>
    <version>${judo-runtime-core-version}</version>
    <scope>test</scope>
</dependency>
```
