# Dispatcher Specification

## Purpose
The dispatcher module is the central request processing engine of JUDO Runtime Core, responsible for routing operation calls to the appropriate behaviour implementations, managing transactions, converting request/response payloads, resolving actors, signing identifiers, and providing environment variable resolution.

## Architecture

### Core Dispatch Pipeline
- `DefaultDispatcher` implements `Dispatcher` and orchestrates the full call lifecycle: operation resolution, access management, actor authentication, request conversion, behaviour dispatch, response conversion, and identifier signing.
- `BehaviourCall` is the interface for all operation behaviour implementations; each implementation declares suitability via `isSuitableForOperation(EOperation)` and executes via `call(Map, EOperation)`.
- `TransactionalBehaviourCall` wraps behaviour execution in a Spring `PlatformTransactionManager` transaction, committing on success and rolling back on exception.
- `AlwaysRollbackTransactionalBehaviourCall` extends `TransactionalBehaviourCall` and always rolls back after execution (used for read-only operations).

### Behaviour Implementations
- `CreateInstanceCall`, `UpdateInstanceCall`, `DeleteInstanceCall` -- CRUD operations extending `TransactionalBehaviourCall`.
- `ListCall`, `RefreshCall`, `GetTemplateCall`, `GetPrincipalCall`, `GetMetadataCall`, `GetReferenceRangeCall`, `GetInputRangeCall` -- read operations extending `AlwaysRollbackTransactionalBehaviourCall`.
- `SetReferenceCall`, `UnsetReferenceCall`, `AddReferenceCall`, `RemoveReferenceCall` -- reference management operations extending `TransactionalBehaviourCall`.
- `ValidateCreateCall`, `ValidateUpdateCall`, `ValidateOperationInputCall` -- validation-only operations.
- `ExportCall`, `GetUploadTokenCall` -- file/export operations.

### Request/Response Conversion
- `RequestConverter` validates and coerces inbound payloads: type coercion via `Coercer`, enumeration literal resolution, binary/file token validation, signed identifier extraction, and payload validation through `PayloadValidator` and `ValidatorProvider`.
- `ResponseConverter` converts outbound payloads: enumeration integer-to-literal mapping, binary-to-download-token conversion, attribute coercion, and structural feature filtering.

### Security Integration
- `ActorResolver` (interface) and `DefaultActorResolver` resolve and authenticate actor instances from principals using DAO queries filtered by USERNAME/EMAIL claims.
- `IdentifierSigner` (interface) and `DefaultIdentifierSigner` produce and verify JWT-signed identifiers using HMAC, RSA, or EC algorithms via JOSE4j.

### Environment Variables
- `VariableResolverManager` (interface) and `DefaultVariableResolver` manage runtime variable providers (suppliers and functions) with optional caching.
- Built-in providers: `CurrentDateProvider`, `CurrentTimeProvider`, `CurrentTimestampProvider`, `EnvironmentVariableProvider`, `PrincipalVariableProvider`, `ActorVariableProvider`, `AccessTokenVariableProvider`, `RequestParametersVariableProvider`, `SequenceProvider`.

### Interceptors
- `OperationCallInterceptor` and `OperationCallInterceptorProvider` allow pre/post interceptors on behaviour calls via `CallInterceptorUtil`.

## Requirements

### Requirement: Operation Routing by Behaviour
The dispatcher SHALL resolve the correct `BehaviourCall` implementation for each `EOperation` by matching the operation's ASM behaviour annotation.

#### Scenario: CRUD operation dispatched to CreateInstanceCall
- **GIVEN** an `EOperation` annotated with behaviour `CREATE_INSTANCE`
- **WHEN** `DefaultDispatcher` processes the operation call
- **THEN** `CreateInstanceCall.isSuitableForOperation` returns true and `CreateInstanceCall.call` is invoked

#### Scenario: List operation dispatched to ListCall
- **GIVEN** an `EOperation` annotated with behaviour `LIST`
- **WHEN** `DefaultDispatcher` processes the operation call
- **THEN** `ListCall.isSuitableForOperation` returns true and `ListCall.callInRollbackTransaction` is invoked

### Requirement: Transactional Behaviour Execution
The dispatcher SHALL execute write operations within a transaction that commits on success and rolls back on exception.

#### Scenario: Successful transactional commit
- **GIVEN** a `TransactionalBehaviourCall` subclass (e.g., `CreateInstanceCall`) with a configured `PlatformTransactionManager`
- **WHEN** `callInTransaction` completes without exception
- **THEN** `transactionManager.commit(transactionStatus)` is called

#### Scenario: Transaction rollback on exception
- **GIVEN** a `TransactionalBehaviourCall` subclass with a configured `PlatformTransactionManager`
- **WHEN** `callInTransaction` throws an exception
- **THEN** `transactionManager.rollback(transactionStatus)` is called and the exception is propagated

### Requirement: Read-Only Operations Always Roll Back
The dispatcher SHALL execute read operations in a transaction that always rolls back to prevent unintended side effects.

#### Scenario: List query rolls back
- **GIVEN** a `ListCall` extending `AlwaysRollbackTransactionalBehaviourCall`
- **WHEN** the list operation completes
- **THEN** the transaction is rolled back regardless of success or failure

### Requirement: Request Payload Validation and Coercion
`RequestConverter` SHALL validate and coerce all inbound payload attributes according to the transfer object type schema.

#### Scenario: String attribute coercion
- **GIVEN** an input payload with a string value for an attribute typed as Integer
- **WHEN** `RequestConverter.convert` processes the payload
- **THEN** the value is coerced to Integer via `Coercer.coerce`

#### Scenario: Enumeration literal validation
- **GIVEN** an input payload with a string literal for an `EEnum`-typed attribute
- **WHEN** `RequestConverter.convert` processes the payload
- **THEN** the literal is resolved to its ordinal value via `AsmUtils`, or an `IllegalArgumentException` is thrown for invalid literals

#### Scenario: String trimming
- **GIVEN** `trimString` is true and the attribute type is string
- **WHEN** `RequestConverter.convert` processes a string value with leading/trailing whitespace
- **THEN** the value is trimmed before storage

#### Scenario: Validation exception on invalid payload
- **GIVEN** `throwValidationException` is true and validation produces errors
- **WHEN** `RequestConverter.convert` processes the payload
- **THEN** a `ValidationException` is thrown containing all `ValidationResult` entries

### Requirement: Response Payload Conversion
`ResponseConverter` SHALL convert outbound payloads by mapping enumeration ordinals to literals, coercing attribute values, and converting binary data to download tokens.

#### Scenario: Enumeration ordinal to literal
- **GIVEN** a DAO result with an integer value for an `EEnum`-typed attribute
- **WHEN** `ResponseConverter.convert` processes the payload
- **THEN** the integer is mapped to the corresponding enumeration literal string

#### Scenario: Binary attribute to download token
- **GIVEN** a DAO result containing a `FileType` for a byte-array attribute
- **WHEN** `ResponseConverter.convert` processes the payload
- **THEN** the `FileType` is converted to a download token string via `TokenIssuer.createDownloadToken`

### Requirement: Actor Authentication and Resolution
`DefaultActorResolver` SHALL authenticate actors by resolving the principal's client type, querying the DAO for matching records, and throwing `AccessDeniedException` when no actor is found.

#### Scenario: Successful actor resolution by USERNAME claim
- **GIVEN** a `JudoPrincipal` with a client name matching a mapped actor type, and the actor type has a USERNAME claim attribute
- **WHEN** `authenticateByPrincipal` is called
- **THEN** the DAO is queried with a filter on the USERNAME attribute and the matching `Payload` is returned

#### Scenario: No matching actor found
- **GIVEN** a `JudoPrincipal` whose claims do not match any actor in the database
- **WHEN** `getActorByClaims` is called
- **THEN** an `AccessDeniedException` is thrown with code `AUTHENTICATED_ENTITY_NOT_FOUND`

#### Scenario: Multiple actors found
- **GIVEN** a DAO query returning more than one result for the given claims
- **WHEN** `getActorByClaims` is called
- **THEN** a `SecurityException` is thrown with message "Multiple actors found in database by token"

### Requirement: Identifier Signing and Verification
`DefaultIdentifierSigner` SHALL produce JWT-signed identifiers for mapped transfer object instances and verify them on inbound requests, supporting HMAC, RSA, and EC algorithms.

#### Scenario: Sign identifier with HMAC
- **GIVEN** `algorithm` is `HMAC_SHA512` and a valid identifier exists in the payload
- **WHEN** `signIdentifiers` is called
- **THEN** a JWT is created with the identifier as subject, the entity type as a claim, and stored under `__signedIdentifier`

#### Scenario: Verify signed identifier
- **GIVEN** a payload containing a `__signedIdentifier` JWT string
- **WHEN** `extractSignedIdentifier` is called
- **THEN** the JWT is verified against the public key, the identifier is extracted and placed into the payload, and a `SignedIdentifier` is returned

#### Scenario: Reject tampered signed identifier
- **GIVEN** a payload containing a modified or invalid `__signedIdentifier`
- **WHEN** `extractSignedIdentifier` is called
- **THEN** an `IllegalStateException` is thrown with message "Invalid signed identifier"

#### Scenario: Reject mismatched signer type
- **GIVEN** a valid signed identifier whose producer type does not match the accessed transfer object type
- **WHEN** `extractSignedIdentifier` is called
- **THEN** an `AccessDeniedException` is thrown with code `ACCESS_DENIED_INVALID_TYPE`

### Requirement: Environment Variable Resolution
`DefaultVariableResolver` SHALL resolve environment variables by category and key, delegating to registered suppliers or functions, with optional per-request caching.

#### Scenario: Resolve cached variable
- **GIVEN** a supplier registered for category "SYSTEM" and key "currentDate" with `cacheable=true`
- **WHEN** `resolve` is called twice in the same request context
- **THEN** the supplier is invoked only once and the cached value is returned on the second call

#### Scenario: Resolve function-based variable
- **GIVEN** a function registered for category "ENV"
- **WHEN** `resolve` is called with category "ENV" and key "MY_VAR"
- **THEN** the function is invoked with key "MY_VAR" and the result is returned

#### Scenario: Undefined variable
- **GIVEN** no supplier or function registered for the requested category/key
- **WHEN** `resolve` is called
- **THEN** null is returned and a warning is logged

### Requirement: Interceptor Pre/Post Call Hooks
The dispatcher SHALL support operation call interceptors that can modify input parameters before execution and transform results after execution.

#### Scenario: Pre-call interceptor modifies input
- **GIVEN** an `OperationCallInterceptor` registered via `OperationCallInterceptorProvider`
- **WHEN** a behaviour call is executed through `CallInterceptorUtil`
- **THEN** `preCallInterceptors` is invoked before the actual call with the input parameter payload

#### Scenario: Post-call interceptor transforms result
- **GIVEN** an `OperationCallInterceptor` registered for the operation
- **WHEN** a behaviour call completes
- **THEN** `postCallInterceptors` is invoked with both the input parameters and the result
