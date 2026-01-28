# JUDO Runtime Core :: Access Manager API

Access control API defining authorization interfaces and contracts.

## Overview

This module defines the core interfaces for access control in JUDO applications. It provides the `AccessManager` interface for authorizing operations and supporting types for authentication interception and identifier signing.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-accessmanager-api</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

**Packaging:** OSGi bundle

## Key Components

### AccessManager
Central interface for operation authorization.

```java
public interface AccessManager {
    /**
     * Authorize an operation call.
     *
     * @param operation operation to call
     * @param signedIdentifier signed identifier of bound operation
     * @param exchange exchange containing principal and context
     */
    void authorizeOperation(EOperation operation, 
                           SignedIdentifier signedIdentifier, 
                           Map<String, Object> exchange);
}
```

### SignedIdentifier
Carries signed entity identifier with operation binding context.

```java
public interface SignedIdentifier {
    // Identifier value
    Object getIdentifier();
    
    // Operation that produced this identifier
    EOperation getProducedBy();
    
    // Signature for validation
    String getSignature();
}
```

### AuthenticationInterceptor
Callback interface for authentication events.

```java
public interface AuthenticationInterceptor {
    boolean isSuitableForOperation(EOperation operation, 
                                   String username, 
                                   String realm, 
                                   String client,
                                   Map<String, Object> attributes);
    
    void success(EOperation operation,
                 SignedIdentifier signedIdentifier,
                 Map<String, Object> exchange,
                 String username,
                 String realm,
                 String client,
                 Map<String, Object> attributes);
}
```

### AuthenticationInterceptorProvider
Provides collection of authentication interceptors.

```java
public interface AuthenticationInterceptorProvider {
    Collection<AuthenticationInterceptor> getAuthenticationInterceptors();
}
```

## Usage Example

```java
// Implement custom access manager
public class CustomAccessManager implements AccessManager {
    @Override
    public void authorizeOperation(EOperation operation, 
                                   SignedIdentifier signedIdentifier, 
                                   Map<String, Object> exchange) {
        Principal principal = (Principal) exchange.get(Dispatcher.PRINCIPAL_KEY);
        
        if (requiresAuthentication(operation) && principal == null) {
            throw new AuthenticationRequiredException(...);
        }
        
        if (!isAuthorized(principal, operation)) {
            throw new AccessDeniedException(...);
        }
    }
}

// Implement authentication interceptor
public class AuditInterceptor implements AuthenticationInterceptor {
    @Override
    public boolean isSuitableForOperation(EOperation op, ...) {
        return true; // Audit all operations
    }
    
    @Override
    public void success(EOperation operation, ...) {
        auditLog.record(operation, username, timestamp);
    }
}
```

## Exception Types

When authorization fails, implementations should throw:

- `AuthenticationRequiredException` - When authentication is required but missing
- `AccessDeniedException` - When authenticated user lacks permission

## Related Modules

- `judo-runtime-core-accessmanager` - Default implementation
- `judo-runtime-core-security` - Security abstractions
- `judo-dispatcher-api` - Dispatcher context and principal
