# Interceptor Registration Capability

## ADDED Requirements

### Requirement: Register Interceptor by Class

Developers can register interceptor classes that will be automatically instantiated and have their dependencies injected.

#### Scenario: Register interceptor class before initialization

- **WHEN** `fixture.addInterceptor(MyInterceptor.class)` is called before `fixture.init()`
- **THEN** the interceptor class is stored for later instantiation
- **AND** during `init()`, the class is instantiated using its no-arg constructor
- **AND** after the Guice injector is created, dependencies are injected via `ReferenceInjector`
- **AND** the interceptor is registered in the `OperationCallInterceptorProvider`

#### Scenario: Interceptor class without no-arg constructor fails fast

- **WHEN** `fixture.addInterceptor(InterceptorWithoutNoArgConstructor.class)` is called
- **AND** `fixture.init()` is called
- **THEN** a `RuntimeException` is thrown with a clear message indicating the constructor issue

### Requirement: Register Interceptor by Instance

Developers can register pre-created interceptor instances for cases requiring custom configuration.

#### Scenario: Register pre-configured interceptor instance

- **WHEN** `fixture.addInterceptor(new MyInterceptor("custom-config"))` is called before `fixture.init()`
- **THEN** the instance is stored
- **AND** during `init()`, dependencies are injected via `ReferenceInjector`
- **AND** the interceptor is registered in the `OperationCallInterceptorProvider`

### Requirement: Interceptors Invoked During Dispatcher Calls

Registered interceptors are automatically invoked when operations are called through the dispatcher.

#### Scenario: Interceptor preCall and postCall are invoked

- **GIVEN** an interceptor is registered with the fixture
- **AND** the fixture is initialized
- **WHEN** `dispatcher.callOperation(...)` is executed
- **THEN** the interceptor's `preCall()` method is invoked before the operation
- **AND** the interceptor's `postCall()` method is invoked after the operation

#### Scenario: Multiple interceptors are invoked in order

- **GIVEN** interceptors A, B, C are registered in that order
- **WHEN** an operation is called
- **THEN** `preCall()` is invoked in order: A, B, C
- **AND** `postCall()` is invoked in order: A, B, C

### Requirement: Annotation-Based Interceptor Registration

The `@JudoTest` annotation supports declarative interceptor registration.

#### Scenario: Register interceptors via annotation

- **GIVEN** a test method annotated with `@JudoTest(interceptors = {MyInterceptor.class})`
- **WHEN** the test is executed
- **THEN** the interceptor is instantiated, injected, and registered automatically
- **AND** the interceptor is invoked during dispatcher calls within the test

#### Scenario: Combine annotation and programmatic registration

- **GIVEN** a test with `@JudoTest(interceptors = {InterceptorA.class})`
- **AND** the test calls `fixture.addInterceptor(InterceptorB.class)` before init
- **WHEN** an operation is called
- **THEN** both InterceptorA and InterceptorB are invoked

### Requirement: Access Interceptor Provider for Advanced Use Cases

Developers can access the underlying `TestOperationCallInterceptorProvider` for advanced manipulation.

#### Scenario: Get interceptor provider after initialization

- **GIVEN** the fixture is initialized
- **WHEN** `fixture.getInterceptorProvider()` is called
- **THEN** the `TestOperationCallInterceptorProvider` instance is returned
- **AND** it can be used to add, remove, or clear interceptors

#### Scenario: Clear all interceptors mid-test

- **GIVEN** interceptors are registered and the fixture is initialized
- **WHEN** `fixture.getInterceptorProvider().clearInterceptors()` is called
- **THEN** subsequent dispatcher calls do not invoke any interceptors
