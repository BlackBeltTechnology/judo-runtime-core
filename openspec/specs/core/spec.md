# JUDO Runtime Core Specification

## Purpose
Provides the foundational runtime abstractions for JUDO-based applications, including data type management with custom type registration, unique identifier generation, performance metrics collection, recursive payload traversal, and a structured exception hierarchy for client-facing error responses.

## Architecture
The module is organized around five key concerns:

- **DataTypeManager** -- Manages custom EDataType registrations and their associated Coercer/Converter/Formatter instances using a ConcurrentHashMap-backed registry.
- **IdentifierProviders** -- Two implementations of the `IdentifierProvider` interface: `UUIDIdentifierProvider` (typed as `UUID`) and `SerializableIdentifierProvider` (typed as `Serializable`), both generating UUIDs and using `__identifier` as the identifier name.
- **MetricsCollector** -- An interface for nested start/stop measurement tracking with an associated `MetricsCancelToken` implementing `AutoCloseable` for try-with-resources usage.
- **PayloadTraverser** -- A builder-based recursive traverser that walks a `Payload` tree along `EReference` relationships, invoking a `BiConsumer<Payload, PayloadTraverserContext>` processor at each node.
- **Exception hierarchy** -- `ClientException` (abstract, extends `RuntimeException`) with concrete subclasses `ValidationException` (400), `AuthenticationRequiredException` (401), `AccessDeniedException` (403), and `NotFoundException` (404).

## Requirements

### Requirement: Custom data type registration
The `DataTypeManager` SHALL allow registration and unregistration of custom `EDataType` instances along with their converters and formatters.

#### Scenario: Registering a new custom type
- **GIVEN** a `DataTypeManager` with an `ExtendableCoercer` and no previously registered custom types
- **WHEN** `registerCustomType(customDataType, customClassName, converters, formatter)` is called
- **THEN** the custom type is stored in the internal registry, all provided converters plus formatter-derived converters are registered with the coercer's converter factory, and `getCustomTypeName(customDataType)` returns `Optional.of(customClassName)`

#### Scenario: Preventing duplicate custom type registration
- **GIVEN** a `DataTypeManager` with a previously registered `EDataType`
- **WHEN** `registerCustomType` is called with the same `EDataType`
- **THEN** an `IllegalStateException` is thrown with the message "Custom type already registered"

#### Scenario: Unregistering a custom type
- **GIVEN** a `DataTypeManager` with a registered custom `EDataType`
- **WHEN** `unregisterCustomType(customDataType)` is called
- **THEN** all associated converters are unregistered from the coercer's converter factory and `getCustomTypeName(customDataType)` returns `Optional.empty()`

### Requirement: UUID-based identifier generation
The `UUIDIdentifierProvider` SHALL generate unique identifiers of type `UUID` using `UUID.randomUUID()`.

#### Scenario: Generating an identifier
- **GIVEN** an instance of `UUIDIdentifierProvider`
- **WHEN** `get()` is called
- **THEN** a non-null `UUID` value is returned

#### Scenario: Identifier type and name
- **GIVEN** an instance of `UUIDIdentifierProvider`
- **WHEN** `getType()` and `getName()` are called
- **THEN** `getType()` returns `UUID.class` and `getName()` returns `"__identifier"`

### Requirement: Serializable identifier generation
The `SerializableIdentifierProvider` SHALL generate unique identifiers typed as `Serializable`.

#### Scenario: Generating a serializable identifier
- **GIVEN** an instance of `SerializableIdentifierProvider`
- **WHEN** `get()` is called
- **THEN** a non-null `Serializable` value (a `UUID`) is returned

#### Scenario: Serializable identifier type and name
- **GIVEN** an instance of `SerializableIdentifierProvider`
- **WHEN** `getType()` and `getName()` are called
- **THEN** `getType()` returns `Serializable.class` and `getName()` returns `"__identifier"`

### Requirement: Metrics collection with nested measurements
The `MetricsCollector` interface SHALL support starting and stopping named measurements and returning accumulated metrics.

#### Scenario: Starting and stopping a measurement
- **GIVEN** a `MetricsCollector` implementation
- **WHEN** `start("myKey")` is called followed by `stop("myKey")`
- **THEN** the elapsed time is recorded and accessible via `getMetrics()` under the key `"myKey"`

#### Scenario: Auto-closing a measurement via MetricsCancelToken
- **GIVEN** a `MetricsCollector` and a `MetricsCancelToken` obtained from `start("myKey")`
- **WHEN** the `MetricsCancelToken` is closed (e.g., in a try-with-resources block)
- **THEN** `stop("myKey")` is called on the underlying `MetricsCollector`

#### Scenario: Stopping with mismatched key
- **GIVEN** a `MetricsCollector` with an active measurement for key `"A"`
- **WHEN** `stop("B")` is called (a different key than the last started)
- **THEN** an `IllegalStateException` is thrown

### Requirement: Recursive payload traversal
The `PayloadTraverser` SHALL recursively traverse a `Payload` tree by following `EReference` relationships on an `EClass`, invoking a processor at each node.

#### Scenario: Traversing a payload with nested references
- **GIVEN** a `PayloadTraverser` configured with a processor `BiConsumer`, a reference predicate, and a `Payload` with nested sub-payloads corresponding to `EReference` values
- **WHEN** `traverse(payload, transferObjectType)` is called
- **THEN** the processor is invoked for the root payload and recursively for each nested payload that matches the predicate, with a `PayloadTraverserContext` carrying the correct `EClass` type and path

#### Scenario: Skipping null payload
- **GIVEN** a `PayloadTraverser`
- **WHEN** `traverse(null, transferObjectType)` is called
- **THEN** `null` is returned and the processor is not invoked

#### Scenario: Path string generation
- **GIVEN** a `PayloadTraverserContext` with a path of `[orders[0], items[2]]`
- **WHEN** `getPathAsString()` is called
- **THEN** the result is `"orders[0].items[2]"`

### Requirement: Client exception hierarchy with HTTP status codes
The `ClientException` abstract class SHALL provide `getStatusCode()` and `getDetails()` methods, and each concrete subclass SHALL map to a specific HTTP status code.

#### Scenario: ValidationException returns status 400
- **GIVEN** a collection of `ValidationResult` objects
- **WHEN** a `ValidationException` is constructed with those results
- **THEN** `getStatusCode()` returns `400` and `getDetails()` returns the collection of `ValidationResult`

#### Scenario: AuthenticationRequiredException returns status 401
- **GIVEN** a `ValidationResult`
- **WHEN** an `AuthenticationRequiredException` is constructed
- **THEN** `getStatusCode()` returns `401` and `getDetails()` returns the `ValidationResult`

#### Scenario: AccessDeniedException returns status 403
- **GIVEN** a `ValidationResult`
- **WHEN** an `AccessDeniedException` is constructed
- **THEN** `getStatusCode()` returns `403` and `getDetails()` returns the `ValidationResult`

#### Scenario: NotFoundException returns status 404
- **GIVEN** a `ValidationResult`
- **WHEN** a `NotFoundException` is constructed
- **THEN** `getStatusCode()` returns `404` and `getDetails()` returns the `ValidationResult`
