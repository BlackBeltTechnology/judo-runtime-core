# Proposal: Add Interceptor Testing Support

## Why

The JUDO testkit currently supports unit testing of interceptors (calling `preCall`/`postCall` directly with `ReferenceInjector`), but **lacks support for integration testing** where interceptors are automatically invoked through the dispatcher flow. Developers need to test that their `OperationCallInterceptor` implementations work correctly when integrated with the full JUDO runtime stack.

This gap forces developers to either skip integration testing or set up complex manual configurations, reducing confidence in interceptor behavior and slowing down development.

## What Changes

- Add a `TestOperationCallInterceptorProvider` utility class for managing interceptors in tests
- Enhance `JudoRuntimeFixture` with methods to register interceptors before initialization
- Add an `interceptors` attribute to the `@JudoTest` annotation for declarative interceptor registration
- Update `JudoTestExtension` to process the new annotation attribute
- Implement deferred dependency injection to solve the chicken-and-egg timing problem

## Capabilities

### New Capabilities

- `interceptor-registration`: Register interceptor classes or instances with the test fixture
- `interceptor-integration-testing`: Test interceptors within the dispatcher flow (automatic invocation)
- `annotation-based-interceptors`: Declare interceptors via `@JudoTest(interceptors = {...})`

### Modified Capabilities

- `judo-runtime-fixture`: Enhanced with `addInterceptor()` methods and deferred injection in `init()`

## Impact

### New Files
- `judo-runtime-core-guice-testkit/src/main/java/.../util/TestOperationCallInterceptorProvider.java`
- `judo-runtime-core-guice-testkit/src/test/java/.../util/TestOperationCallInterceptorProviderTest.java`
- `judo-runtime-core-guice-testkit/src/test/java/.../fixture/JudoRuntimeFixtureInterceptorTest.java`

### Modified Files
- `judo-runtime-core-guice-testkit/src/main/java/.../fixture/JudoRuntimeFixture.java`
- `judo-runtime-core-guice-testkit/src/main/java/.../fixture/JudoTest.java`
- `judo-runtime-core-guice-testkit/src/main/java/.../fixture/JudoTestExtension.java`
- `judo-runtime-core-guice-testkit/src/main/java/.../examples/InterceptorIntegrationTest.java`

## Test Coverage

### TestOperationCallInterceptorProvider Tests

**Positive Tests:**
- Add single interceptor and verify registration
- Add multiple interceptors and verify order preserved
- Remove interceptor and verify removal
- Clear all interceptors
- Get interceptors returns defensive copy

**Negative Tests:**
- Add null interceptor throws IllegalArgumentException
- Remove non-existent interceptor returns false

### JudoRuntimeFixture Interceptor Tests

**Positive Tests:**
- Register interceptor by class before init
- Register interceptor by instance before init
- Register multiple interceptors (mixed class and instance)
- Interceptors receive dependency injection after init
- Get interceptor provider after init

**Negative Tests:**
- Add interceptor after init throws IllegalStateException
- Add null interceptor class throws IllegalArgumentException
- Add null interceptor instance throws IllegalArgumentException
- Add interceptor class without no-arg constructor fails with clear message
- Get interceptor provider before init throws IllegalStateException

**Combination Tests:**
- Register by class + by instance together
- Clear interceptors mid-test and verify no invocations
- Add interceptor via provider after init (runtime modification)
