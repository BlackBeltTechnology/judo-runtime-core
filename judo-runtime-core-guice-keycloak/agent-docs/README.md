# JUDO Guice Keycloak Module

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-guice-keycloak
- **Version**: ${project.version}

## Overview

This module provides Google Guice integration for Keycloak, enabling OAuth2/OpenID Connect authentication, realm synchronization, user management, and password policies.

## Main Components

### JudoKeycloakModule

The primary Guice module that configures Keycloak integration.

```java
import hu.blackbelt.judo.runtime.core.security.keycloak.guice.JudoKeycloakModule;

// Basic usage with defaults
Injector injector = Guice.createInjector(
    JudoKeycloakModule.builder().build()
);

// Production configuration
Injector injector = Guice.createInjector(
    JudoKeycloakModule.builder()
        .keycloakServerUrl("https://auth.example.com/auth")
        .keycloakPublicUrl("https://auth.example.com/auth")
        .keycloakAdminUser("admin")
        .keycloakAdminPassword(System.getenv("KEYCLOAK_ADMIN_PASSWORD"))
        .keycloakUserManagerEnabled(true)
        .build()
);
```

### Configuration Options

#### Server Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keycloakServerUrl` | String | `"http://localhost:8080/auth"` | Internal Keycloak URL |
| `keycloakPublicUrl` | String | `"http://localhost:8080/auth"` | Public-facing Keycloak URL |
| `keycloakAdminUser` | String | `"admin"` | Admin username |
| `keycloakAdminPassword` | String | `"judo"` | Admin password |
| `keycloakClientSecret` | String | `""` | Client secret for confidential clients |

#### Realm Synchronizer Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keycloakRealmSynchronizerSupportLoginByEmail` | Boolean | `true` | Allow email login |
| `keycloakRealmSynchronizerClientAccessTypeForHuman` | String | `"CONFIDENTIAL"` | Access type for human users |
| `keycloakRealmSynchronizerClientAccessTypeForSystem` | String | `"BEARER_ONLY"` | Access type for system clients |
| `keycloakRealmSynchronizerCorsAllowOrigin` | String | `"*"` | CORS allowed origins |
| `keycloakRealmSynchronizerAsyncServiceCall` | Boolean | `true` | Async realm sync |
| `keycloakRealmSynchronizerRetryMaxAttempts` | Integer | `1000` | Max retry attempts |
| `keycloakRealmSynchronizerRetryExponentialBackoff` | Boolean | `true` | Use exponential backoff |
| `keycloakRealmSynchronizerRetryWaitDuration` | Long | `1000` | Wait duration (ms) |

#### User Manager Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keycloakUserManagerEnabled` | Boolean | `true` | Enable user management |
| `keycloakUserManagerUpdateExistingUsers` | Boolean | `false` | Update existing users |
| `keycloakUserManagerRequiredActions` | String | `""` | Required user actions |
| `keycloakUserManagerAsyncServiceCall` | Boolean | `true` | Async user operations |
| `keycloakUserManagerRetryMaxAttempts` | Integer | `1000` | Max retry attempts |
| `keycloakUserManagerRetryExponentialBackoff` | Boolean | `true` | Use exponential backoff |
| `keycloakUserManagerRetryWaitDuration` | Long | `1000` | Wait duration (ms) |

#### Security Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keycloakSecurityPasswordPolicyType` | String | `"NO_PASSWORD"` | Password policy type |

### Provided Bindings

| Type | Implementation | Scope |
|------|----------------|-------|
| `KeycloakAdminClient` | Admin API client | Eager Singleton |
| `KeycloakConnector` | Token validation/exchange | Eager Singleton |
| `KeycloakUserManager` | User CRUD operations | Eager Singleton |
| `KeycloakRealmSynchronizer` | Realm sync from model | Eager Singleton |
| `PasswordPolicy` | Password validation | Eager Singleton |
| `OpenIdConfigurationProvider` | OIDC configuration | Eager Singleton |
| `RealmExtractor` | Extract realm from request | Eager Singleton |

The module also registers a `KeycloakLoginInterceptor` as a CXF in-interceptor.

## Maven Dependency

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-guice-keycloak</artifactId>
    <version>${project.version}</version>
</dependency>
```

## Full Stack Example

```java
Injector injector = Guice.createInjector(
    // Database
    JudoPostgresqlModule.builder()
        .host("localhost")
        .databaseName("myapp")
        .build(),
    // Core runtime
    JudoDefaultModule.builder()
        .asmModel(asmModel)
        .rdbmsModel(rdbmsModel)
        .build(),
    // HTTP server
    JudoJettyModule.builder()
        .jettyServerPort(8080)
        .build(),
    // REST API
    JudoCxfModule.builder()
        .cxfJaxRsServerPath("api")
        .build(),
    // Authentication
    JudoKeycloakModule.builder()
        .keycloakServerUrl("http://keycloak:8080/auth")
        .keycloakPublicUrl("https://auth.myapp.com/auth")
        .keycloakAdminUser("admin")
        .keycloakAdminPassword(System.getenv("KC_ADMIN_PASSWORD"))
        .keycloakUserManagerEnabled(true)
        .build()
);
```

## TestContainers Integration

```java
@Container
static KeycloakContainer keycloak = new KeycloakContainer("quay.io/keycloak/keycloak:17.0.1");

@BeforeAll
static void setup() {
    injector = Guice.createInjector(
        JudoKeycloakModule.builder()
            .keycloakServerUrl(keycloak.getAuthServerUrl())
            .keycloakPublicUrl(keycloak.getAuthServerUrl())
            .keycloakAdminUser(keycloak.getAdminUsername())
            .keycloakAdminPassword(keycloak.getAdminPassword())
            .build()
    );
}
```

## Configuration Qualifiers

```java
import hu.blackbelt.judo.runtime.core.security.keycloak.guice.KeycloakConfigurationQualifiers;

@Inject
@KeycloakConfigurationQualifiers.KeycloakServerUrl
private String serverUrl;

@Inject
@KeycloakConfigurationQualifiers.KeycloakUserManagerEnabled
private Boolean userManagerEnabled;
```

## Extending the Module

Override configuration methods by extending:

```java
public class CustomKeycloakModule extends JudoKeycloakModule {
    @Override
    public void configureAdditional() {
        // Add custom bindings
        bind(MyCustomAuthService.class).asEagerSingleton();
    }
}
```
