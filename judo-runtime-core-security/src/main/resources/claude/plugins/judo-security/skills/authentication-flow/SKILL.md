---
name: judo-runtime:authentication-flow
description: Comprehensive guide to the JUDO authentication pipeline. Use when you need to understand how authentication works, debug auth issues, configure OAuth2/OpenID Connect, or integrate with identity providers like Keycloak.
metadata:
  author: BlackBelt Technology
  version: "${project.version}"
---

# JUDO Authentication Flow

Complete guide to understanding the authentication pipeline in JUDO Runtime Core.

## Overview

JUDO Security provides a multi-layer authentication system supporting:
- OAuth2 / OpenID Connect (OIDC) authentication
- JWT token validation and claims extraction
- Multi-tenant / multi-realm support
- Actor-based access control
- User lifecycle management

## Architecture

```mermaid
graph TB
    subgraph "Client"
        A[HTTP Request]
    end
    
    subgraph "CXF Layer"
        B[KeycloakLoginInterceptor]
    end
    
    subgraph "Security Core"
        C[RealmExtractor]
        D[OpenIdConfigurationProvider]
        E[UserManager]
    end
    
    subgraph "Identity Provider"
        F[Keycloak Server]
    end
    
    subgraph "Application"
        G[Business Logic]
        H[DAO Layer]
    end
    
    A -->|Authorization: Bearer token| B
    B --> C
    C -->|Extract actor type| B
    B --> D
    D -->|Get OIDC config| F
    B -->|Validate JWT| F
    B -->|Set SecurityContext| G
    G --> H
    H --> E
    E -->|Sync users| F
```

## Authentication Sequence

```mermaid
sequenceDiagram
    participant Client
    participant Interceptor as KeycloakLoginInterceptor
    participant Extractor as RealmExtractor
    participant OIDC as OpenIdConfigurationProvider
    participant Keycloak
    participant App as Application
    
    Client->>Interceptor: HTTP Request + Bearer Token
    Interceptor->>Extractor: extractActorType(request)
    Extractor-->>Interceptor: Optional<EClass> actorType
    
    alt Actor Type Found
        Interceptor->>OIDC: getOpenIdConfiguration(actorType)
        OIDC-->>Interceptor: OIDC Config (issuer, jwks_uri, etc.)
        Interceptor->>Keycloak: Validate JWT using JWKS
        Keycloak-->>Interceptor: Token validation result
        
        alt Valid Token
            Interceptor->>Interceptor: Extract claims, map to attributes
            Interceptor->>App: SecurityContext with JudoPrincipal
            App-->>Client: Response
        else Invalid Token
            Interceptor-->>Client: 401 Unauthorized
        end
    else Anonymous Access
        Interceptor->>App: No SecurityContext
        App-->>Client: Response (if allowed)
    end
```

## Core Interfaces

### OpenIdConfigurationProvider

Provides OpenID Connect configuration for each actor type:

```java
public interface OpenIdConfigurationProvider {
    
    // Get the .well-known/openid-configuration URL for an actor type
    String getOpenIdConfigurationUrl(EClass actorType);
    
    // Get parsed OIDC configuration (cached)
    Map<String, Object> getOpenIdConfiguration(EClass actorType);
    
    // Get the identity provider server URL
    String getServerUrl();
    
    // Get OAuth2 client ID for an actor type
    String getClientId(EClass actorType);
    
    // Health check for identity provider
    void ping();
}
```

### RealmExtractor

Extracts actor type from HTTP requests:

```java
public interface RealmExtractor {
    
    // Extract actor type from request (typically from URL path)
    Optional<EClass> extractActorType(HttpServletRequest request);
}
```

**Default Implementation: PathInfoRealmExtractor**

Extracts actor from URL path:
- Request: `POST /api/Model/Actor/operation`
- Extracted: `Model.Actor` actor type

### UserManager

Manages users in the identity provider:

```java
public interface UserManager<ID> {
    
    // Get user by username
    Optional<Map<String, Object>> getUser(EClass actor, ID username);
    
    // Get all users for an actor type
    List<Map<String, Object>> getAllUsers(EClass actor);
    
    // Create a new user
    void createUser(EClass actor, Map<String, Object> user);
    
    // Update existing user
    void updateUser(EClass actor, ID username, Map<String, Object> user);
    
    // Delete user
    void deleteUser(EClass actor, ID username);
    
    // Get managed actor for a principal type
    Optional<EClass> getManagedActorOfPrincipal(EClass principalType);
    
    // Get attribute mapping between principal and Keycloak
    Map<String, String> getPrincipalAttributeMapping(EClass principalType);
    
    // Extract username from user data
    Optional<ID> getUsername(EClass principalType, Map<String, Object> user);
}
```

### PasswordPolicy

Defines password generation rules:

```java
public interface PasswordPolicy<ID> extends Function<Map<String, Object>, Optional<ID>> {
    // Apply policy: user data -> optional generated password
}
```

**Implementations:**
- `NoPasswordPolicy` - No automatic password generation
- `SameUsernamePasswordPolicy` - Password equals username
- `SameEmailPasswordPolicy` - Password equals email

## Token Flow

```mermaid
graph LR
    subgraph "JWT Token"
        A[Header]
        B[Payload/Claims]
        C[Signature]
    end
    
    subgraph "Claim Mapping"
        D[preferred_username]
        E[email]
        F[custom claims]
    end
    
    subgraph "JudoPrincipal"
        G[name]
        H[realm]
        I[client]
        J[attributes]
    end
    
    B --> D
    B --> E
    B --> F
    
    D --> G
    D --> J
    E --> J
    F --> J
```

## User Synchronization

The `UserManagedWrappedDao` automatically syncs users with the identity provider:

```mermaid
sequenceDiagram
    participant App as Application
    participant Wrapper as UserManagedWrappedDao
    participant DAO as Delegate DAO
    participant UserMgr as UserManager
    participant Keycloak
    
    App->>Wrapper: create(Actor, payload)
    Wrapper->>DAO: create(Actor, payload)
    DAO-->>Wrapper: Created entity
    Wrapper->>UserMgr: createUser(actor, userData)
    UserMgr->>Keycloak: POST /admin/realms/{realm}/users
    Keycloak-->>UserMgr: 201 Created
    Wrapper-->>App: Created entity
    
    Note over Wrapper,UserMgr: Same flow for update/delete
```

## Configuration

### Keycloak Module Configuration (Guice)

```java
JudoKeycloakModuleConfiguration.builder()
    .keycloakServerUrl("http://localhost:8080/auth")
    .keycloakExternalUrl("https://auth.example.com")
    .keycloakAdminUser("admin")
    .keycloakAdminPassword("admin")
    .keycloakClientSecret(null)  // Optional for confidential clients
    .keycloakUserManagerEnabled(true)
    .keycloakUpdateExistingUsers(false)
    .keycloakAsyncServiceCall(true)  // Async user management calls
    .keycloakRetryMaxAttempts(1000)
    .keycloakRetryWaitDuration(1000L)
    .keycloakRetryExponentialBackoff(true)
    .build();
```

### Actor Type Annotations

Actor types require these annotations in the ASM model:

```
@actorType(managed=true)  // Enable user management
@realm("my-realm")        // Keycloak realm name
@claim("USERNAME")        // Attribute mapped to Keycloak username
@claim("EMAIL")           // Attribute mapped to Keycloak email
```

## Security Context

After successful authentication, `JudoPrincipal` is available:

```java
// In CXF resource
@Context
SecurityContext securityContext;

public Response myOperation() {
    JudoPrincipal principal = (JudoPrincipal) securityContext.getUserPrincipal();
    
    String username = principal.getName();
    String realm = principal.getRealm();
    String client = principal.getClient();  // Actor FQN
    Map<String, Object> claims = principal.getAttributes();
    
    // Use principal for authorization
}
```

## Error Handling

| Error | HTTP Status | Description |
|-------|-------------|-------------|
| Missing token | Depends on endpoint | Anonymous access (may be allowed) |
| Expired token | 401 | `ACCESS_TOKEN_EXPIRED` error code |
| Invalid signature | 401 | Token verification failed |
| Unknown realm | N/A | Request processed as anonymous |

## Debugging

### Enable Debug Logging

```xml
<logger name="hu.blackbelt.judo.runtime.core.security" level="DEBUG"/>
<logger name="org.keycloak" level="DEBUG"/>
```

### Common Issues

1. **Token validation fails**
   - Check Keycloak server URL matches issuer in token
   - Verify JWKS endpoint is accessible
   - Check clock skew between servers

2. **User not created in Keycloak**
   - Verify `keycloakUserManagerEnabled=true`
   - Check actor type has `@actorType(managed=true)`
   - Review Keycloak admin credentials

3. **Claims not mapped**
   - Verify `@claim` annotations on actor attributes
   - Check `AttributeBinding` in Keycloak model
   - Enable DEBUG logging to see token contents

## See Also

- `judo-runtime-core-security-keycloak` - Keycloak implementation
- `judo-runtime-core-security-keycloak-cxf` - CXF interceptors
- `judo-runtime-core-guice-keycloak` - Guice configuration
- `judo-runtime-core-accessmanager-api` - Authorization APIs
