# Access Manager Specification

## Purpose
The accessmanager module provides the default implementation of the `AccessManager` interface, enforcing operation-level authorization based on ASM model annotations, actor exposure rules, CRUD permission flags, and signed identifier provenance verification.

## Architecture

### Key Classes
- `DefaultAccessManager` -- implements `AccessManager`, orchestrating authorization checks: public/private actor resolution, operation exposure verification, signed identifier provenance checks, authentication interceptor callbacks, and per-behaviour CRUD flag validation via `BehaviourAuthorizer` chain.
- `BehaviourAuthorizer` (abstract) -- base class for behaviour-specific authorization. Provides `isSuitableForOperation(EOperation)` and `authorize(actorFqName, publicActors, signedIdentifier, operation)`. Contains `checkCRUDFlag` utility that verifies CREATE/UPDATE/DELETE permission annotations on model elements.
- Concrete authorizers: `ListAuthorizer`, `CreateInstanceAuthorizer`, `UpdateInstanceAuthorizer`, `DeleteInstanceAuthorizer`, `RefreshAuthorizer`, `SetReferenceAuthorizer`, `UnsetReferenceAuthorizer`, `AddReferenceAuthorizer`, `RemoveReferenceAuthorizer`, `GetReferenceRangeAuthorizer`, `GetInputRangeAuthorizer`, `GetTemplateAuthorizer`.

### Authorization Flow
1. `DefaultAccessManager.authorizeOperation` checks that the principal is a `JudoPrincipal`.
2. Verifies that the operation is exposed to the calling actor (via `exposedBy` annotations) or is a metadata/principal operation.
3. Verifies signed identifier provenance (if present) is also exposed to the actor.
4. Invokes `AuthenticationInterceptor.success` callbacks for suitable interceptors.
5. Delegates to the appropriate `BehaviourAuthorizer` to check CRUD permission flags.

## Requirements

### Requirement: Actor Exposure Verification
`DefaultAccessManager` SHALL verify that the called operation is exposed to the authenticated actor or a public actor.

#### Scenario: Operation exposed to authenticated actor
- **GIVEN** an operation with `exposedBy` annotation listing the actor's FQN
- **WHEN** `authorizeOperation` is called with a matching `JudoPrincipal`
- **THEN** authorization proceeds without exception

#### Scenario: Operation exposed to public actor
- **GIVEN** an operation with `exposedBy` annotation listing a public actor (no realm), and no principal is provided
- **WHEN** `authorizeOperation` is called
- **THEN** authorization proceeds without exception

#### Scenario: Operation not exposed to actor
- **GIVEN** an operation whose `exposedBy` annotations do not include the calling actor or any public actor
- **WHEN** `authorizeOperation` is called with an authenticated principal
- **THEN** an `AccessDeniedException` is thrown with code `ACCESS_DENIED`

#### Scenario: Non-public operation without authentication
- **GIVEN** an operation not exposed to any public actor, and no principal is provided
- **WHEN** `authorizeOperation` is called
- **THEN** an `AuthenticationRequiredException` is thrown with code `AUTHENTICATION_REQUIRED`

### Requirement: Principal Operation Requires Authentication
`DefaultAccessManager` SHALL require a valid principal for `GET_PRINCIPAL` operations.

#### Scenario: Get principal without token
- **GIVEN** an operation with behaviour `GET_PRINCIPAL` and no principal in the exchange
- **WHEN** `authorizeOperation` is called
- **THEN** an `AuthenticationRequiredException` is thrown with code `INVALID_TOKEN`

### Requirement: Signed Identifier Provenance Verification
`DefaultAccessManager` SHALL verify that the signed identifier's producing element is exposed to the calling actor.

#### Scenario: Signed identifier exposed to actor
- **GIVEN** a signed identifier whose `producedBy` element has `exposedBy` matching the actor
- **WHEN** `authorizeOperation` is called
- **THEN** authorization proceeds

#### Scenario: Signed identifier not exposed to actor
- **GIVEN** a signed identifier whose `producedBy` element's `exposedBy` does not match the actor or any public actor
- **WHEN** `authorizeOperation` is called
- **THEN** an `AccessDeniedException` is thrown with code `ACCESS_DENIED_FOR_INSTANCE_OF_BOUND_OPERATION`

### Requirement: CRUD Permission Flag Enforcement
`BehaviourAuthorizer` implementations SHALL check the `permissions` annotation on model elements for the required CRUD flags.

#### Scenario: Create operation with CREATE permission
- **GIVEN** an operation with behaviour `CREATE_INSTANCE`, and the owning reference has `permissions` annotation with `create=true`
- **WHEN** `CreateInstanceAuthorizer.authorize` is called
- **THEN** the `CREATE` flag check passes

#### Scenario: Create operation without CREATE permission
- **GIVEN** an operation with behaviour `CREATE_INSTANCE`, and the owning reference has `permissions` annotation with `create=false`
- **WHEN** `CreateInstanceAuthorizer.authorize` is called
- **THEN** an `AccessDeniedException` is thrown with code `PERMISSION_DENIED` and details containing `MISSING_PRIVILEGES=[CREATE]`

#### Scenario: Bound create requires UPDATE on producer
- **GIVEN** a bound `CREATE_INSTANCE` operation (owner not annotated as access) with a signed identifier
- **WHEN** `CreateInstanceAuthorizer.authorize` is called
- **THEN** the `UPDATE` CRUD flag is also checked on the `signedIdentifier.producedBy` element

#### Scenario: Delete operation permission check
- **GIVEN** an operation with behaviour `DELETE_INSTANCE`
- **WHEN** `DeleteInstanceAuthorizer.authorize` is called
- **THEN** the `DELETE` CRUD flag is checked on the owning reference

#### Scenario: Update operation permission check
- **GIVEN** an operation with behaviour `UPDATE_INSTANCE`
- **WHEN** `UpdateInstanceAuthorizer.authorize` is called
- **THEN** the `UPDATE` CRUD flag is checked on the owning reference

### Requirement: Behaviour Authorizer Routing
`DefaultAccessManager` SHALL delegate to the correct `BehaviourAuthorizer` based on the operation's ASM behaviour annotation.

#### Scenario: List operation routed to ListAuthorizer
- **GIVEN** an operation with behaviour `LIST`
- **WHEN** `authorizeOperation` iterates through authorizers
- **THEN** `ListAuthorizer.isSuitableForOperation` returns true and `ListAuthorizer.authorize` is invoked

#### Scenario: Reference operations routed correctly
- **GIVEN** an operation with behaviour `SET_REFERENCE`
- **WHEN** `authorizeOperation` iterates through authorizers
- **THEN** `SetReferenceAuthorizer.isSuitableForOperation` returns true and its `authorize` is invoked

### Requirement: Authentication Interceptor Success Callbacks
`DefaultAccessManager` SHALL invoke `AuthenticationInterceptor.success` for all suitable interceptors after successful authorization.

#### Scenario: Interceptor success callback
- **GIVEN** an `AuthenticationInterceptor` registered via `AuthenticationInterceptorProvider` that is suitable for the operation
- **WHEN** `authorizeOperation` completes exposure and CRUD checks successfully
- **THEN** `interceptor.success(operation, signedIdentifier, exchange, claim, realm, client, attributes)` is invoked

### Requirement: Public Actor Resolution
`DefaultAccessManager` SHALL identify public actors as those actor types without a realm annotation value.

#### Scenario: Actor without realm is public
- **GIVEN** an actor type `EClass` with no `realm` annotation or an empty realm value
- **WHEN** `DefaultAccessManager` is constructed
- **THEN** the actor's FQN is added to the `publicActors` set
