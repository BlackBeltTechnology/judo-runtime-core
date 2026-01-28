# JUDO Security Keycloak CXF

## Overview

This module provides Apache CXF integration for Keycloak authentication. It includes:

- **KeycloakLoginInterceptor** - CXF interceptor for JWT token validation
- **KeycloakSecurityContext** - Security context wrapper for authenticated principals

The interceptor extracts Bearer tokens from HTTP requests, validates them against Keycloak, and populates the CXF security context with the authenticated principal.

## Key Components

### KeycloakLoginInterceptor

CXF phase interceptor that validates Keycloak JWT tokens and establishes security context.

```java
KeycloakLoginInterceptor interceptor = KeycloakLoginInterceptor.builder()
    .realmExtractor(realmExtractor)
    .asmModel(asmModel)
    .openIdConfigurationProvider(keycloakConnector)
    .transformationTraceService(traceService)
    .build();

// Register with CXF endpoint
endpoint.getInInterceptors().add(interceptor);
```

**Interceptor Phase**: `UNMARSHAL`

**Authentication Flow**:
1. Extract actor type from request using `RealmExtractor`
2. Get realm name from actor type annotations
3. Extract Bearer token from `Authorization` header
4. Validate token using Keycloak adapter
5. Map token claims to actor attributes
6. Set `SecurityContext` with `JudoPrincipal`

### KeycloakSecurityContext

Implements CXF `SecurityContext` interface, wrapping a `JudoPrincipal`.

```java
KeycloakSecurityContext context = KeycloakSecurityContext.builder()
    .userPrincipal(JudoPrincipal.builder()
        .name("username")
        .realm("my-realm")
        .client("my-app")
        .attributes(tokenAttributes)
        .build())
    .build();

// Check role (uses 'azp' claim - authorized party / issued for)
boolean hasRole = context.isUserInRole("my-app");

// Get principal
JudoPrincipal principal = context.getUserPrincipal();
```

## Authentication Flow

```
HTTP Request
    |
    v
+-------------------+
|  Extract Actor    |  RealmExtractor determines actor type from request
|  Type from URL    |
+-------------------+
    |
    v
+-------------------+
|  Get Realm from   |  Actor type has @realm annotation
|  Actor Annotation |
+-------------------+
    |
    v
+-------------------+
|  Extract Bearer   |  Authorization: Bearer <token>
|  Token            |
+-------------------+
    |
    v
+-------------------+
|  Build Keycloak   |  AdapterConfig with auth-server-url, realm, resource
|  Deployment       |
+-------------------+
    |
    v
+-------------------+
|  Verify Token     |  AdapterTokenVerifier validates signature and expiry
+-------------------+
    |
    v
+-------------------+
|  Map Claims to    |  AttributeBinding maps token claims to actor attributes
|  Attributes       |
+-------------------+
    |
    v
+-------------------+
|  Create Security  |  JudoPrincipal with username, realm, client, attributes
|  Context          |
+-------------------+
    |
    v
+-------------------+
|  Set MDC User     |  MDC.put("user", username) for logging
+-------------------+
```

## Token Validation

The interceptor uses Keycloak's `AdapterTokenVerifier` which:

- Verifies JWT signature using realm public key
- Checks token expiration
- Validates issuer matches the realm

### Keycloak Deployment Configuration

For each realm, a `KeycloakDeployment` is created with:

| Setting | Value |
|---------|-------|
| `connectionPoolSize` | 10 |
| `authServerUrl` | From `OpenIdConfigurationProvider` |
| `realm` | From actor type annotation |
| `resource` | Actor type FQN (client ID) |
| `sslRequired` | `external` |
| `publicClient` | `true` |
| `confidentialPort` | `0` |

## Claim Mapping

Token claims are mapped to actor attributes using `AttributeBinding` from the Keycloak model:

```
Token Claim          Actor Attribute
------------         ----------------
preferred_username   username (if claim type is USERNAME)
email                email (if claim type is EMAIL)
azp                  (used for role checking)
...                  (custom bindings from model)
```

## Error Handling

### Token Expired

```java
AuthenticationRequiredException
  - code: "ACCESS_TOKEN_EXPIRED"
  - level: ERROR
```

### Invalid Token

```java
AuthenticationException("Authentication failed")
```

Exceptions caught:
- `TokenNotActiveException` - Token expired
- `VerificationException` - Signature invalid
- `JWSInputException` - Malformed token
- `IOException` - Token parsing error

## Integration with CXF

### JAX-RS Endpoint

```java
JAXRSServerFactoryBean factory = new JAXRSServerFactoryBean();
factory.setServiceBean(myResource);
factory.getInInterceptors().add(keycloakLoginInterceptor);
Server server = factory.create();
```

### Guice Integration

When using `judo-runtime-core-guice-keycloak`, the interceptor is automatically registered:

```java
Injector injector = Guice.createInjector(
    new JudoKeycloakModule(JudoKeycloakModuleConfiguration.builder()
        .keycloakServerUrl("http://keycloak:8080/auth")
        .keycloakAdminUser("admin")
        .keycloakAdminPassword("admin")
        .build())
);

KeycloakLoginInterceptor interceptor = injector.getInstance(KeycloakLoginInterceptor.class);
```

## Required Dependencies

This module requires:

- `judo-runtime-core-security` - Core security interfaces
- `judo-runtime-core-security-keycloak` - Keycloak connector (for `OpenIdConfigurationProvider`)
- `keycloak-adapter-core` - Keycloak token verification
- `keycloak-core` - Keycloak data types
- `cxf-core` - Apache CXF interceptor framework

## Security Considerations

1. **Token Validation**: All tokens are validated against Keycloak public keys
2. **SSL**: SSL is required for external connections (`sslRequired=external`)
3. **Logging**: Username is added to MDC for audit logging
4. **Debug Mode**: Token content is only logged at DEBUG level

## Troubleshooting

### Token Validation Failures

Enable debug logging to see token details:

```xml
<logger name="hu.blackbelt.judo.runtime.core.security.keycloak.cxf" level="DEBUG"/>
```

### Missing Realm

If the actor type doesn't have a `@realm` annotation, authentication is skipped (anonymous access).

### Clock Skew

Token expiration is strict. Ensure server clocks are synchronized with Keycloak.

## Related Modules

- `judo-runtime-core-security` - Core security interfaces
- `judo-runtime-core-security-keycloak` - Keycloak connector
- `judo-runtime-core-guice-keycloak` - Guice bindings
- `judo-runtime-core-jaxrs-cxf` - CXF JAX-RS integration
