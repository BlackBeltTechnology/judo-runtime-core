# Security Keycloak Specification

## Purpose
The security-keycloak module provides the Keycloak-specific implementation of the JUDO security abstractions, including user management against Keycloak's Admin REST API, OpenID Connect configuration from Keycloak realms, realm synchronization, and password policies.

## Architecture

### Key Classes
- `KeycloakUserManager` -- implements `UserManager<String>`, performing CRUD operations on Keycloak users within the realm associated with each actor type. Supports async execution, Resilience4j retry, and attribute binding between ASM model attributes and Keycloak user attributes.
- `KeycloakConnector` -- implements `OpenIdConfigurationProvider`, manages Keycloak server connectivity, admin authentication (username/password for admin-cli), OpenID configuration retrieval with optional external URL rewriting, and health checks.
- `KeycloakAdminClient` -- low-level client wrapping CXF `WebClient` calls to Keycloak Admin REST API endpoints for realms, clients, and users.
- `KeycloakRealmSynchronizer` -- synchronizes ASM-defined Keycloak realms and clients with the actual Keycloak server, creating or updating as needed. Supports async execution with Resilience4j retry.
- `SameUsernamePasswordPolicy` -- `PasswordPolicy<String>` that sets the default password to the user's username.
- `SameEmailPasswordPolicy` -- `PasswordPolicy<String>` that sets the default password to the user's email.
- `RetryUtil` -- utility for creating Resilience4j `RetryRegistry` with configurable max attempts, wait duration, and exponential backoff.

## Requirements

### Requirement: Keycloak User CRUD Operations
`KeycloakUserManager` SHALL manage users in Keycloak realms corresponding to ASM actor types, supporting create, read, update, and delete operations.

#### Scenario: Create user in Keycloak
- **GIVEN** a `KeycloakUserManager` with `enabled=true` and a managed actor type with a realm mapping
- **WHEN** `createUser(actorType, userData)` is called
- **THEN** a POST request is sent to Keycloak's `/admin/realms/{realm}/users` endpoint with the user payload including enabled=true, username, optional credentials, and optional required actions

#### Scenario: Update existing user on create when updateExistingUsers is true
- **GIVEN** `updateExistingUsers=true` and a user with the same username already exists in Keycloak
- **WHEN** `createUser(actorType, userData)` is called
- **THEN** the existing user is updated via PUT to `/admin/realms/{realm}/users/{userId}` instead of creating a new one

#### Scenario: Delete user from Keycloak
- **GIVEN** a managed actor type and a user exists in Keycloak
- **WHEN** `deleteUser(actorType, username)` is called
- **THEN** the user is found by username and a DELETE request is sent to `/admin/realms/{realm}/users/{userId}`

#### Scenario: User manager disabled
- **GIVEN** `enabled=false` on `KeycloakUserManager`
- **WHEN** any user operation is called
- **THEN** the operation is skipped with an info log message

#### Scenario: Async user creation with retry
- **GIVEN** `asyncServiceCall=true` on `KeycloakUserManager`
- **WHEN** `createUser` is called
- **THEN** the Keycloak REST call is executed asynchronously via `CompletableFuture.runAsync` with Resilience4j retry decoration

### Requirement: Identity Manager Readiness Gate
`KeycloakUserManager` SHALL enforce that user operations only execute when the identity manager is marked as ready.

#### Scenario: Operation before identity manager ready
- **GIVEN** `identityManagerReady=false`
- **WHEN** the internal create/update/delete runnable executes
- **THEN** an `IllegalStateException` is thrown by `checkState(identityManagerReady)`

### Requirement: OpenID Connect Configuration from Keycloak
`KeycloakConnector` SHALL provide OpenID Connect configuration by querying Keycloak's well-known endpoint for each actor type's realm.

#### Scenario: Retrieve OpenID configuration
- **GIVEN** a `KeycloakConnector` with a realm mapped to an actor type
- **WHEN** `getOpenIdConfiguration(actorType)` is called
- **THEN** a GET request is made to `/realms/{realm}/.well-known/openid-configuration` and the response map is returned

#### Scenario: External URL rewriting
- **GIVEN** an `externalUrl` is configured on `KeycloakConnector`
- **WHEN** `getOpenIdConfiguration(actorType)` is called
- **THEN** all URLs in the response that start with `serverUrl` are rewritten to use `externalUrl`

#### Scenario: Configuration caching
- **GIVEN** `getOpenIdConfiguration` was previously called for an actor type
- **WHEN** `getOpenIdConfiguration` is called again for the same actor type
- **THEN** the cached configuration map is returned without a new HTTP request

#### Scenario: Health check ping
- **GIVEN** a `KeycloakConnector`
- **WHEN** `ping()` is called
- **THEN** a GET request is made to the server root; `IllegalStateException` is thrown if the response status is not 200 or 302

### Requirement: Keycloak Admin Authentication
`KeycloakConnector` SHALL authenticate with the Keycloak master realm to obtain an admin access token for Admin REST API calls.

#### Scenario: Obtain admin access token
- **GIVEN** valid `adminUser` and `adminPassword` configured
- **WHEN** `getAdminClient()` is called
- **THEN** a POST to `/realms/master/protocol/openid-connect/token` is made with grant_type=password, and the resulting access_token is used as a Bearer token for subsequent admin requests

### Requirement: Realm and Client Synchronization
`KeycloakRealmSynchronizer` SHALL synchronize Keycloak realms and clients defined in the Keycloak model with the actual Keycloak server.

#### Scenario: Create new realm
- **GIVEN** a realm defined in the Keycloak model that does not exist on the server
- **WHEN** `synchronizeAllRealms` is called
- **THEN** a new realm is created via `KeycloakAdminClient.createOrUpdateRealm(realm, false)`

#### Scenario: Update existing realm
- **GIVEN** a realm exists on the server but has different enabled/loginWithEmailAllowed settings
- **WHEN** `synchronizeAllRealms` is called
- **THEN** the realm is updated via `KeycloakAdminClient.createOrUpdateRealm(realm, true)`

#### Scenario: Create new client in realm
- **GIVEN** a client defined in the Keycloak model that does not exist in the realm
- **WHEN** `synchronizeAllRealms` is called
- **THEN** a new client is created with appropriate access type (PUBLIC, CONFIDENTIAL, or BEARER_ONLY) based on the actor kind (HUMAN vs. SYSTEM)

#### Scenario: Identity manager ready callback
- **GIVEN** `registerIdentityManagerReady` callback is configured
- **WHEN** realm synchronization completes successfully
- **THEN** the callback is invoked to signal identity manager readiness

### Requirement: Default Password Policies
Password policy implementations SHALL derive initial passwords from user data.

#### Scenario: Same username password policy
- **GIVEN** a `SameUsernamePasswordPolicy` and user data containing `username=john`
- **WHEN** `apply(userData)` is called
- **THEN** `Optional.of("john")` is returned

#### Scenario: Same email password policy
- **GIVEN** a `SameEmailPasswordPolicy` and user data containing `email=john@example.com`
- **WHEN** `apply(userData)` is called
- **THEN** `Optional.of("john@example.com")` is returned

### Requirement: Principal Attribute Mapping
`KeycloakUserManager` SHALL map principal (transfer object) attributes to Keycloak user attributes using transformation trace bindings.

#### Scenario: Resolve attribute mapping for principal type
- **GIVEN** a managed actor type with attribute bindings defined via transformation trace
- **WHEN** `getPrincipalAttributeMapping(principalType)` is called
- **THEN** a map of Keycloak attribute names to principal attribute names is returned

#### Scenario: Username resolution from principal data
- **GIVEN** a principal type with a USERNAME or EMAIL claim mapped to an actor attribute
- **WHEN** `getUsername(principalType, userData)` is called
- **THEN** the username is extracted from user data using the mapped attribute name
