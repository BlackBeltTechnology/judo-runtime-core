# Tasks: Add Interceptor Testing Support

## 1. Create TestOperationCallInterceptorProvider

- [x] 1.1 Create `TestOperationCallInterceptorProvider.java` in `util/` package
- [x] 1.2 Implement `OperationCallInterceptorProvider` interface
- [x] 1.3 Add `addInterceptor(OperationCallInterceptor)` method
- [x] 1.4 Add `removeInterceptor(OperationCallInterceptor)` method
- [x] 1.5 Add `clearInterceptors()` method
- [x] 1.6 Add `getInterceptors()` method returning a copy of the list

## 2. Enhance JudoRuntimeFixture

- [x] 2.1 Add `interceptorProvider` field of type `TestOperationCallInterceptorProvider`
- [x] 2.2 Add `interceptorClasses` field as `List<Class<? extends OperationCallInterceptor>>`
- [x] 2.3 Add `interceptorInstances` field as `List<OperationCallInterceptor>`
- [x] 2.4 Add `addInterceptor(Class<? extends OperationCallInterceptor>)` method
- [x] 2.5 Add `addInterceptor(OperationCallInterceptor)` method for instances
- [x] 2.6 Add `getInterceptorProvider()` method
- [x] 2.7 Modify `init()` to create and configure `TestOperationCallInterceptorProvider`
- [x] 2.8 Modify `init()` to instantiate interceptor classes
- [x] 2.9 Modify `init()` to pass provider to `judoDefaultModuleBuilder`
- [x] 2.10 Modify `init()` to perform deferred injection after injector creation

## 3. Enhance @JudoTest Annotation

- [x] 3.1 Add `interceptors()` attribute with default empty array
- [x] 3.2 Add Javadoc explaining the attribute behavior

## 4. Enhance JudoTestExtension

- [x] 4.1 In `beforeEach()`, read `interceptors` attribute from annotation
- [x] 4.2 Register each interceptor class with `runtimeFixture.addInterceptor()`
- [x] 4.3 Add debug logging for registered interceptors

## 5. Update Example Tests

- [x] 5.1 Update `InterceptorIntegrationTest.java` with working dispatcher-flow example
- [x] 5.2 Add example showing annotation-based interceptor registration
- [x] 5.3 Add example showing programmatic interceptor registration

## 6. Create JUnit Tests for TestOperationCallInterceptorProvider

- [x] 6.1 Create `TestOperationCallInterceptorProviderTest.java`
- [x] 6.2 Test: Add single interceptor and verify registration
- [x] 6.3 Test: Add multiple interceptors and verify order preserved
- [x] 6.4 Test: Remove interceptor and verify removal
- [x] 6.5 Test: Clear all interceptors
- [x] 6.6 Test: Get interceptors returns defensive copy
- [x] 6.7 Test: Add null interceptor throws IllegalArgumentException
- [x] 6.8 Test: Remove non-existent interceptor returns false

## 7. Create JUnit Tests for JudoRuntimeFixture Interceptor Support

- [x] 7.1 Create `JudoRuntimeFixtureInterceptorTest.java`
- [x] 7.2 Test: Register interceptor by class before init
- [x] 7.3 Test: Register interceptor by instance before init
- [x] 7.4 Test: Register multiple interceptors (mixed class and instance)
- [x] 7.5 Test: Get interceptor provider after init
- [x] 7.6 Test: Add interceptor after init throws IllegalStateException
- [x] 7.7 Test: Add null interceptor class throws IllegalArgumentException
- [x] 7.8 Test: Add null interceptor instance throws IllegalArgumentException
- [x] 7.9 Test: Add interceptor class without no-arg constructor fails
- [x] 7.10 Test: Get interceptor provider before init throws IllegalStateException
- [x] 7.11 Test: Combination - register by class + by instance together
- [x] 7.12 Test: Combination - clear interceptors mid-test
