# Security Core Specification

## Purpose
The security module defines the core security abstractions for JUDO Runtime Core, including user management, realm extraction from HTTP requests, password policy contracts, OpenID Connect configuration provisioning, and a DAO wrapper that synchronizes user lifecycle events with an external identity manager.

## Architecture

### Interfaces
- `UserManager<ID>` -- generic interface for CRUD operations on users in an external identity store, mapping between ASM actor types and the identity provider. Methods: `getUser`, `getAllUsers`, `createUser`, `updateUser`, `deleteUser`, `getManagedActorOfPrincipal`, `getPrincipalAttributeMapping`, `getUsername`.
- `RealmExtractor` -- extracts the actor type (realm) from an `HttpServletRequest`. Single method: `extractActorType(HttpServletRequest)`.
- `PasswordPolicy<ID>` -- a `Function<Map<String, Object>, Optional<ID>>` that derives a default password from user data.
- `OpenIdConfigurationProvider` -- provides OpenID Connect configuration URLs, configuration maps, server URLs, client IDs, and health checks for actor types.

### Implementations
- `PathInfoRealmExtractor` -- extracts actor type by matching the request `pathInfo` against all known actor types from the ASM model (pattern: `/<package>/<ActorType>/...`).
- `NoPasswordPolicy<ID>` -- a no-op password policy that always returns `Optional.empty()`.
- `UserManagedWrappedDao` -- a `DAO` decorator that intercepts `create`, `update`, and `delete` operations to synchronize corresponding user records in the `UserManager`. It delegates all read operations and reference operations directly to the underlying DAO.

## Requirements

### Requirement: User Management Lifecycle
`UserManager` SHALL provide CRUD operations for users in the external identity store, keyed by actor type and username.

#### Scenario: Retrieve user by username
- **GIVEN** a `UserManager` implementation and a valid actor type
- **WHEN** `getUser(actorType, username)` is called
- **THEN** the user data is returned as `Optional<Map<String, Object>>`, or `Optional.empty()` if not found

#### Scenario: Create user
- **GIVEN** a `UserManager` implementation, a valid actor type, and user data
- **WHEN** `createUser(actorType, userData)` is called
- **THEN** a new user is created in the external identity store with the provided data

#### Scenario: Delete user
- **GIVEN** a `UserManager` implementation, a valid actor type, and a username
- **WHEN** `deleteUser(actorType, username)` is called
- **THEN** the user is removed from the external identity store

### Requirement: Realm Extraction from HTTP Requests
`RealmExtractor` SHALL extract the actor type from the HTTP request to determine the security realm for authentication.

#### Scenario: Path-based realm extraction
- **GIVEN** a `PathInfoRealmExtractor` initialized with an ASM model containing actor types
- **WHEN** `extractActorType` is called with a request whose `pathInfo` is `/com/example/MyActor/someOperation`
- **THEN** the `EClass` for `com.example.MyActor` is returned

#### Scenario: No matching actor type
- **GIVEN** a `PathInfoRealmExtractor` and a request with `pathInfo` that matches no actor
- **WHEN** `extractActorType` is called
- **THEN** `Optional.empty()` is returned

#### Scenario: Null path info
- **GIVEN** a request with null `pathInfo`
- **WHEN** `extractActorType` is called
- **THEN** `Optional.empty()` is returned

### Requirement: Password Policy Abstraction
`PasswordPolicy` SHALL derive a default password from user data, returning `Optional.empty()` when no password should be set.

#### Scenario: No password policy applied
- **GIVEN** a `NoPasswordPolicy` instance
- **WHEN** `apply(userData)` is called with any user data
- **THEN** `Optional.empty()` is returned

### Requirement: OpenID Connect Configuration Provisioning
`OpenIdConfigurationProvider` SHALL provide OpenID Connect well-known configuration and metadata for each actor type.

#### Scenario: Retrieve OpenID configuration URL
- **GIVEN** an `OpenIdConfigurationProvider` implementation and a known actor type
- **WHEN** `getOpenIdConfigurationUrl(actorType)` is called
- **THEN** the well-known OpenID configuration URL is returned

#### Scenario: Health check
- **GIVEN** an `OpenIdConfigurationProvider` implementation
- **WHEN** `ping()` is called
- **THEN** the identity provider's availability is verified, throwing `IllegalStateException` if unreachable

### Requirement: DAO User Synchronization
`UserManagedWrappedDao` SHALL intercept DAO create, update, and delete operations to synchronize user records with the `UserManager` for managed actor types.

#### Scenario: User created on DAO create
- **GIVEN** a `UserManagedWrappedDao` wrapping a delegate DAO with a configured `UserManager`
- **WHEN** `create(clazz, payload, queryCustomizer)` is called for a managed actor type
- **THEN** the delegate `create` is called first, then `UserManager.createUser` is called with the actor data

#### Scenario: User updated on DAO update
- **GIVEN** a `UserManagedWrappedDao` with a configured `UserManager`
- **WHEN** `update(clazz, payload, queryCustomizer)` is called for a managed actor type
- **THEN** the existing username is resolved, the delegate `update` is called, and `UserManager.updateUser` is called

#### Scenario: User deleted on DAO delete
- **GIVEN** a `UserManagedWrappedDao` with a configured `UserManager`
- **WHEN** `delete(clazz, id)` is called for a managed actor type
- **THEN** the username is resolved, the delegate `delete` is called, and `UserManager.deleteUser` is called

#### Scenario: Username immutability enforced
- **GIVEN** an update to a managed actor payload
- **WHEN** the loaded username after delegate update differs from the original username
- **THEN** an `IllegalArgumentException` is thrown with message "Username is not changeable"

#### Scenario: Non-managed type passes through
- **GIVEN** a DAO operation on a type that is not a managed actor
- **WHEN** any CRUD method is called
- **THEN** the operation is delegated directly without `UserManager` interaction

#### Scenario: UserManager not ready
- **GIVEN** `userManager` is null and `userManagerEnabled` is true
- **WHEN** a create, update, or delete operation is called
- **THEN** an `IllegalArgumentException` is thrown with message "User manager is not started yet"
