# JUDO Runtime Core :: Security

Core security framework providing authentication and authorization abstractions.

## Overview

This module defines the security abstractions and implementations for JUDO applications. It provides interfaces for password policies, realm extraction, OpenID configuration, and user management that integrate with external identity providers.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-security</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

**Packaging:** OSGi bundle

## Key Components

### OpenIdConfigurationProvider
Interface for retrieving OpenID Connect configuration from identity providers.

```java
public interface OpenIdConfigurationProvider {
    String getOpenIdConfigurationUrl(EClass actorType);
    Map<String, Object> getOpenIdConfiguration(EClass actorType);
    String getServerUrl();
    String getClientId(EClass actorType);
    void ping();
}
```

### PasswordPolicy
Interface for password validation rules.

```java
public interface PasswordPolicy {
    void validatePassword(String password);
}
```

### NoPasswordPolicy
Default implementation that accepts any password (no validation).

### RealmExtractor
Interface for extracting security realm information from requests.

### PathInfoRealmExtractor
Implementation that extracts realm from URL path information.

### UserManager
Interface for user lifecycle management operations.

### UserManagedWrappedDao
DAO wrapper that adds user management context to data operations.

## Usage Example

```java
// Implement custom password policy
public class StrongPasswordPolicy implements PasswordPolicy {
    @Override
    public void validatePassword(String password) {
        if (password.length() < 12) {
            throw new ValidationException("Password must be at least 12 characters");
        }
        // Additional validation rules...
    }
}

// Use realm extractor
RealmExtractor extractor = new PathInfoRealmExtractor();
String realm = extractor.extractRealm(httpServletRequest);
```

## Integration Points

This module integrates with:
- **judo-dao-api** - DAO abstractions
- **judo-dispatcher-api** - Dispatcher context
- **judo-meta-asm** - ASM model for actor type definitions

## Related Modules

- `judo-runtime-core-security-keycloak` - Keycloak OAuth2/OpenID Connect implementation
- `judo-runtime-core-security-keycloak-cxf` - Keycloak + CXF integration
- `judo-runtime-core-accessmanager-api` - Access control API
