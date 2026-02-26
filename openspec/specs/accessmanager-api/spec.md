# Access Manager API Specification

## Purpose
The accessmanager-api module defines the contracts for authorization and access control in JUDO Runtime Core, providing interfaces for operation-level authorization, signed identifier tracking, and authentication/authorization interceptor extension points.

## Architecture

### Key Interfaces and Classes
- `AccessManager` -- the primary authorization interface with a single method `authorizeOperation(EOperation, SignedIdentifier, Map<String, Object>)` that checks whether a given operation call is permitted.
- `SignedIdentifier` -- an immutable value object (Lombok `@Builder`/`@Getter`) carrying the verified identity of a data instance: `identifier` (string, required), `producedBy` (the `ETypedElement` that produced the identifier), `entityType` (the underlying entity type name), `version` (optional optimistic locking version), and `immutable` (optional flag).
- `AuthenticationInterceptor` -- an SPI for custom authentication/authorization hooks with two callback methods: `authenticate` (called after principal extraction, before actor load) and `success` (called after successful authorization, before operation execution).
- `AuthenticationInterceptorProvider` -- provides a collection of `AuthenticationInterceptor` instances. Default implementation returns an empty list.

## Requirements

### Requirement: Operation Authorization Contract
`AccessManager` SHALL provide a single entry point to authorize operation calls, receiving the operation, the signed identifier of the bound instance (if applicable), and the request exchange.

#### Scenario: Authorize permitted operation
- **GIVEN** an `AccessManager` implementation and an operation exposed to the calling actor
- **WHEN** `authorizeOperation(operation, signedIdentifier, exchange)` is called
- **THEN** the method returns normally without exception

#### Scenario: Deny unauthorized operation
- **GIVEN** an `AccessManager` implementation and an operation not exposed to the calling actor
- **WHEN** `authorizeOperation(operation, signedIdentifier, exchange)` is called
- **THEN** an exception is thrown indicating access denial

### Requirement: Signed Identifier Value Object
`SignedIdentifier` SHALL carry the verified identity of a data instance with its provenance information.

#### Scenario: Build signed identifier
- **GIVEN** an identifier string, a producing `ETypedElement`, an entity type, a version, and an immutable flag
- **WHEN** `SignedIdentifier.builder().identifier(...).producedBy(...).entityType(...).version(...).immutable(...).build()` is called
- **THEN** a `SignedIdentifier` is created with all fields accessible via getters

#### Scenario: Identifier is required
- **GIVEN** a `SignedIdentifier.builder()` without an identifier
- **WHEN** `build()` is called
- **THEN** a `NullPointerException` is thrown due to the `@NonNull` constraint on `identifier`

### Requirement: Authentication Interceptor Extension Point
`AuthenticationInterceptor` SHALL allow custom code to hook into the authentication and authorization lifecycle.

#### Scenario: Authenticate callback invoked
- **GIVEN** an `AuthenticationInterceptor` registered via `AuthenticationInterceptorProvider`
- **WHEN** the dispatcher processes a request with a valid principal
- **THEN** `authenticate(operationFQN, exchange, claim, realm, client, attributes)` is called after principal extraction but before actor data is loaded

#### Scenario: Success callback invoked after authorization
- **GIVEN** an `AuthenticationInterceptor` registered and suitable for the operation
- **WHEN** the access manager successfully authorizes the operation
- **THEN** `success(operation, signedIdentifier, exchange, claim, realm, client, attributes)` is called

#### Scenario: Suitability check
- **GIVEN** an `AuthenticationInterceptor` with a custom `isSuitableForOperation` implementation
- **WHEN** the access manager evaluates interceptors
- **THEN** only interceptors returning true from `isSuitableForOperation` have their `success` callback invoked

### Requirement: Authentication Interceptor Provider
`AuthenticationInterceptorProvider` SHALL provide a collection of registered `AuthenticationInterceptor` instances.

#### Scenario: Default provider returns empty collection
- **GIVEN** the default `AuthenticationInterceptorProvider` implementation
- **WHEN** `getAuthenticationInterceptors()` is called
- **THEN** an empty `ArrayList` is returned
