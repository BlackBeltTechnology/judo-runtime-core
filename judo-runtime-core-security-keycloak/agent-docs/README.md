# JUDO Security Keycloak

## Overview

This module provides Keycloak OAuth2/OpenID Connect integration for JUDO applications. It includes:

- **KeycloakConnector** - OpenID configuration provider and Keycloak API client
- **KeycloakAdminClient** - Admin operations for realms, clients, and users
- **KeycloakUserManager** - User management with automatic Keycloak synchronization
- **KeycloakRealmSynchronizer** - Realm and client synchronization with Keycloak
- **Password Policies** - Built-in password policies for user creation

## Key Components

### KeycloakConnector

Main entry point for Keycloak integration. Implements `OpenIdConfigurationProvider` interface.

```java
KeycloakConnector connector = KeycloakConnector.builder()
    .asmModel(asmModel)
    .objectMapper(objectMapper)
    .serverUrl("http://localhost:8080/auth")
    .externalUrl("https://auth.example.com/auth")  // Optional: public URL
    .adminUser("admin")
    .adminPassword("admin")
    .clientSecret("client-secret")  // Optional
    .build();

// Get OpenID configuration for an actor type
Map<String, Object> config = connector.getOpenIdConfiguration(actorType);

// Check Keycloak availability
connector.ping();
```

### KeycloakAdminClient

Provides administrative operations against Keycloak Admin REST API.

```java
KeycloakAdminClient adminClient = KeycloakAdminClient.builder()
    .keycloakConnector(connector)
    .build();

// List realms
List<Realm> realms = adminClient.getListOfRealms();

// Get users in a realm
List<Map<String, Object>> users = adminClient.getUsersOfRealm("my-realm");

// Get specific user
Optional<Map<String, Object>> user = adminClient.getUserOfRealm("my-realm", "username");

// Create/update realm
adminClient.createOrUpdateRealm(realm, false);  // false = create, true = update

// Create/update client
adminClient.createOrUpdateClient("my-realm", client, false);
```

### KeycloakUserManager

User lifecycle management with automatic Keycloak synchronization. Implements `UserManager<String>` interface.

```java
KeycloakUserManager userManager = KeycloakUserManager.builder()
    .asmModel(asmModel)
    .defaultPasswordPolicy(new SameUsernamePasswordPolicy())
    .transformationTraceService(traceService)
    .transformationTrace(trace)
    .keycloakAdminClient(adminClient)
    .enabled(true)
    .updateExistingUsers(false)
    .requiredActions("VERIFY_EMAIL,UPDATE_PASSWORD")
    .asyncServiceCall(true)
    .retryMaxAttempts(1000)
    .retryExponentialBackoff(true)
    .retryWaitDuration(1000L)
    .build();

// Create user
userManager.createUser(actorType, userData);

// Update user
userManager.updateUser(actorType, "username", userData);

// Delete user
userManager.deleteUser(actorType, "username");

// Get user
Optional<Map<String, Object>> user = userManager.getUser(actorType, "username");
```

### KeycloakRealmSynchronizer

Synchronizes realm and client configurations from JUDO model to Keycloak.

```java
KeycloakRealmSynchronizer synchronizer = KeycloakRealmSynchronizer.builder()
    .keycloakAdminClient(adminClient)
    .keycloakModel(keycloakModel)
    .transformationTraceService(traceService)
    .transformationTrace(trace)
    .systemDefaultAccessType(AccessType.CONFIDENTIAL)
    .humanDefaultSystemAccessType(AccessType.BEARER_ONLY)
    .supportLoginByEmail(true)
    .corsAllowOrigin(Set.of("*"))
    .asyncServiceCall(true)
    .retryMaxAttempts(1000)
    .retryExponentialBackoff(true)
    .retryWaitDuration(1000L)
    .registerIdentityManagerReady(() -> userManager.setIdentityManagerReady(true))
    .build();

// Synchronize all realms
synchronizer.synchronizeAllRealms();
```

## Configuration Options

### KeycloakConnector Configuration

| Parameter | Type | Description |
|-----------|------|-------------|
| `serverUrl` | String | Keycloak server URL (internal) |
| `externalUrl` | String | Keycloak public URL (optional, for external access) |
| `adminUser` | String | Admin username for Keycloak API |
| `adminPassword` | String | Admin password for Keycloak API |
| `clientSecret` | String | Client secret (optional, for confidential clients) |

### KeycloakUserManager Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | Boolean | `true` | Enable/disable user management |
| `updateExistingUsers` | Boolean | `false` | Update existing users on create |
| `requiredActions` | String | `null` | Comma-separated required actions (e.g., `VERIFY_EMAIL,UPDATE_PASSWORD`) |
| `asyncServiceCall` | Boolean | `true` | Execute operations asynchronously |
| `retryMaxAttempts` | Integer | `1000` | Maximum retry attempts |
| `retryExponentialBackoff` | Boolean | `true` | Use exponential backoff |
| `retryWaitDuration` | Long | `1000` | Initial wait duration in ms |

### KeycloakRealmSynchronizer Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `systemDefaultAccessType` | AccessType | `CONFIDENTIAL` | Default access type for system actors |
| `humanDefaultSystemAccessType` | AccessType | `BEARER_ONLY` | Default access type for human actors |
| `supportLoginByEmail` | Boolean | `true` | Allow login with email |
| `corsAllowOrigin` | Collection | `empty` | CORS allowed origins |
| `asyncServiceCall` | Boolean | `true` | Execute synchronization asynchronously |
| `retryMaxAttempts` | Integer | `1000` | Maximum retry attempts |
| `retryExponentialBackoff` | Boolean | `true` | Use exponential backoff |
| `retryWaitDuration` | Long | `1000` | Initial wait duration in ms |

### Access Types

| Type | Description |
|------|-------------|
| `PUBLIC` | Public client (no secret required) |
| `CONFIDENTIAL` | Confidential client (secret required) |
| `BEARER_ONLY` | Bearer-only client (no direct login) |

## Password Policies

### SameUsernamePasswordPolicy

Sets the initial password to the username value.

```java
PasswordPolicy<String> policy = new SameUsernamePasswordPolicy();
```

### SameEmailPasswordPolicy

Sets the initial password to the email value.

```java
PasswordPolicy<String> policy = new SameEmailPasswordPolicy();
```

## Environment Variables (Guice Integration)

When using with `judo-runtime-core-guice-keycloak`:

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `KEYCLOAK_SERVER_URL` | `http://localhost:8080/auth` | Keycloak server URL |
| `KEYCLOAK_PUBLIC_URL` | Same as server URL | Keycloak public URL |
| `KEYCLOAK_ADMIN_USER` | `admin` | Admin username |
| `KEYCLOAK_ADMIN_PASSWORD` | `judo` | Admin password |
| `KEYCLOAK_CLIENT_SECRET` | (empty) | Client secret |
| `KEYCLOAK_USER_MANAGER_ENABLED` | `true` | Enable user management |
| `KEYCLOAK_USER_MANAGER_UPDATE_EXISTING` | `false` | Update existing users |
| `KEYCLOAK_USER_MANAGER_REQUIRED_ACTIONS` | (empty) | Required actions |
| `KEYCLOAK_ASYNC_SERVICE_CALL` | `true` | Async operations |
| `KEYCLOAK_RETRY_MAX_ATTEMPTS` | `1000` | Max retry attempts |
| `KEYCLOAK_RETRY_EXPONENTIAL_BACKOFF` | `true` | Exponential backoff |
| `KEYCLOAK_RETRY_WAIT_DURATION` | `1000` | Wait duration (ms) |
| `KEYCLOAK_PASSWORD_POLICY_TYPE` | `NO_PASSWORD` | Password policy type |

## Resilience and Retry

This module uses Resilience4j for retry handling. All Keycloak operations automatically retry on:

- `NotFoundException`
- `ConnectException`
- `ProcessingException`
- `IllegalStateException`

## Related Modules

- `judo-runtime-core-security` - Core security interfaces
- `judo-runtime-core-security-keycloak-cxf` - Keycloak + CXF interceptors
- `judo-runtime-core-guice-keycloak` - Guice bindings for Keycloak

## Troubleshooting

### Keycloak Connection Issues

```java
try {
    connector.ping();
} catch (IllegalStateException e) {
    // Keycloak is not available
}
```

### User Creation Failures

Enable debug logging to see detailed error messages:

```xml
<logger name="hu.blackbelt.judo.runtime.core.security.keycloak" level="DEBUG"/>
```

### Realm Synchronization

The synchronizer creates realms and clients if they don't exist, or updates them if they do. Check Keycloak admin console for created entities.
