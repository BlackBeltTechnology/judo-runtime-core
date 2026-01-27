# Design: Add Interceptor Testing Support

## Context

The JUDO runtime uses `OperationCallInterceptor` to decorate operation calls with cross-cutting concerns (audit logging, permission checks, data transformation, etc.). The testkit needs to support testing these interceptors within the dispatcher flow, not just in isolation.

The core challenge is a **dependency injection timing problem**: interceptors need dependencies from the Guice injector, but the interceptor provider must be configured before the injector is created.

## Goals / Non-Goals

### Goals

- Enable integration testing of interceptors within the dispatcher flow
- Maintain backward compatibility with existing testkit usage
- Support both programmatic and annotation-based interceptor registration
- Solve the dependency injection timing problem transparently

### Non-Goals

- Change how interceptors are written (must remain compatible with existing `@Reference` pattern)
- Support hot-swapping interceptors in production code (test-only feature)
- Provide interceptor mocking utilities (developers can use standard mocking frameworks)

## Decisions

### Decision 1: Deferred Injection Pattern

**Approach:** Register interceptor instances before the injector is created, then inject their dependencies after.

**Flow:**
```
1. User calls fixture.addInterceptor(MyInterceptor.class)
   → Class stored in interceptorClasses list

2. User calls fixture.init()
   → Instantiate interceptor classes (fields are null)
   → Register instances in TestOperationCallInterceptorProvider
   → Build JudoDefaultModule with custom provider
   → Create Guice injector
   → DEFERRED: Inject dependencies into all interceptor instances

3. Dispatcher calls operation
   → Interceptor's preCall/postCall invoked with working dependencies
```

**Rationale:** This approach:
- Doesn't require changing how interceptors are written
- Maintains compatibility with existing `@Reference` pattern
- Hides complexity from test authors
- Follows the existing pattern of `ReferenceInjector`

### Decision 2: TestOperationCallInterceptorProvider as Mutable Implementation

**Approach:** Create a new `TestOperationCallInterceptorProvider` class that implements `OperationCallInterceptorProvider` with mutable `add/remove/clear` methods.

**Rationale:** The default `OperationCallInterceptorProviderProvider` creates an anonymous implementation with an effectively immutable list. A dedicated test implementation allows:
- Adding interceptors programmatically
- Removing interceptors mid-test for negative testing
- Clearing all interceptors
- Exposing the list for inspection/verification

### Decision 3: Interceptor Classes Must Have No-Arg Constructor

**Approach:** When registering by class, require a public no-arg constructor. Fail fast with a clear error message if not present.

**Rationale:**
- Dependencies are injected via field/setter injection, not constructor injection
- Consistent with OSGi service component pattern
- Interceptors requiring constructor parameters can be registered as pre-created instances

### Decision 4: Annotation Attribute for Declarative Registration

**Approach:** Add `interceptors` attribute to `@JudoTest` annotation.

```java
@JudoTest(interceptors = { MyInterceptor.class, AnotherInterceptor.class })
void testWithInterceptors(JudoRuntimeFixture fixture) { ... }
```

**Rationale:**
- Consistent with existing `modules` attribute pattern
- Declarative and visible at test method level
- Reduces boilerplate for common use cases

### Decision 5: Interceptor Lifecycle is Per-Test

**Approach:** Interceptors are created fresh for each test execution.

**Rationale:**
- Follows the fixture lifecycle (fixture is per-test)
- Prevents state leakage between tests
- Consistent with test isolation principles

## Component Structure

```
TestOperationCallInterceptorProvider
├── addInterceptor(interceptor)
├── removeInterceptor(interceptor)
├── clearInterceptors()
├── getInterceptors() → List
└── getCallOperationInterceptors() → Collection  [interface method]

JudoRuntimeFixture (enhanced)
├── interceptorClasses: List<Class<?>>
├── interceptorInstances: List<OperationCallInterceptor>
├── interceptorProvider: TestOperationCallInterceptorProvider
├── addInterceptor(Class<?>)  [NEW]
├── addInterceptor(instance)  [NEW]
├── getInterceptorProvider()  [NEW]
└── init()  [MODIFIED - adds deferred injection]

@JudoTest (enhanced)
└── interceptors: Class<?>[]  [NEW]

JudoTestExtension (enhanced)
└── beforeEach()  [MODIFIED - processes interceptors attribute]
```

## Sequence Diagram

```
Test                    JudoRuntimeFixture           TestInterceptorProvider    Guice
 |                              |                              |                  |
 |--addInterceptor(class)------>|                              |                  |
 |                              |--store in classes list       |                  |
 |                              |                              |                  |
 |--init(module, target)------->|                              |                  |
 |                              |--instantiate classes-------->|                  |
 |                              |--addInterceptor(instance)--->|                  |
 |                              |                              |                  |
 |                              |--build module with provider->|                  |
 |                              |                              |                  |
 |                              |--createInjector(modules)-------------------->|
 |                              |<---------------------------------injector----|
 |                              |                              |                  |
 |                              |--inject dependencies-------->|                  |
 |                              |  (ReferenceInjector)         |                  |
 |<-----------------------------|                              |                  |
 |                              |                              |                  |
 |--dispatcher.call()---------->|                              |                  |
 |                              |         preCall()<-----------|                  |
 |                              |         postCall()<----------|                  |
```
