# Guice Keycloak Module Specification

## Purpose
Provides a Google Guice module (`JudoKeycloakModule`) that integrates Keycloak OAuth2/OpenID Connect identity management into the JUDO runtime, including authentication interceptors, realm synchronization, user management, password policy, and admin client configuration.

## Architecture
- **JudoKeycloakModule** (`hu.blackbelt.judo.runtime.core.security.keycloak.guice.JudoKeycloakModule`): `AbstractModule` that configures all Keycloak-related bindings including login interceptor, admin client, connector, user manager, realm synchronizer, and realm extractor.
- **JudoKeycloakModuleConfiguration** (`hu.blackbelt.judo.runtime.core.security.keycloak.guice.JudoKeycloakModuleConfiguration`): Lombok `@Builder` configuration for Keycloak server URL, admin credentials, client secret, realm synchronizer settings, user manager settings, and retry/resilience parameters.
- **KeycloakConfigurationQualifiers** (`hu.blackbelt.judo.runtime.core.security.keycloak.guice.KeycloakConfigurationQualifiers`): Binding annotations for all Keycloak configuration values.

### Provider Classes
| Provider | Binds | Scope |
|----------|-------|-------|
| `KeycloakLoginInterceptorProvider` | CXF in-interceptor for Keycloak login | Eager Singleton |
| `KeycloakPasswordPolicyProvider` | `PasswordPolicy` | Eager Singleton |
| `KeycloakAdminClientProvider` | `KeycloakAdminClient` | Eager Singleton |
| `KeycloakConnectorProvider` | `KeycloakConnector` | Eager Singleton |
| `KeycloakConnectorOpenIdConfigurationProviderProvider` | `OpenIdConfigurationProvider` | Eager Singleton |
| `KeycloakUserManagerProvider` | `KeycloakUserManager` | Eager Singleton |
| `KeycloakRealmSynchronizerProvider` | `KeycloakRealmSynchronizer` | Eager Singleton |
| `Asm2KeycloakTransformationProvider` | ASM-to-Keycloak transformation | - |
| `PathInfoRealmExtractorProvider` | `RealmExtractor` | Eager Singleton |

## Requirements

### Requirement: Keycloak Login Interceptor
The module SHALL register a Keycloak login interceptor as a CXF in-interceptor via the `Multibinder` mechanism for `@CxfQualifiers.InInterceptors`.

#### Scenario: Login interceptor is registered
- **GIVEN** a `JudoKeycloakModule` installed in the Guice injector
- **WHEN** `configureKeycloakLoginInterceptor()` is invoked
- **THEN** `KeycloakLoginInterceptorProvider` SHALL be added to the `@CxfQualifiers.InInterceptors` multibinder set as an eager singleton

### Requirement: Password Policy via Keycloak
The module SHALL bind `PasswordPolicy` to `KeycloakPasswordPolicyProvider` for enforcing Keycloak-managed password policies.

#### Scenario: Keycloak password policy is bound
- **GIVEN** a `JudoKeycloakModule` is configured
- **WHEN** `configurePasswordPolicy()` is invoked
- **THEN** `PasswordPolicy` SHALL be bound via `KeycloakPasswordPolicyProvider` as an eager singleton

### Requirement: Keycloak Admin Client
The module SHALL bind `KeycloakAdminClient` as an eager singleton for administrative operations against the Keycloak server.

#### Scenario: Admin client is bound
- **GIVEN** a `JudoKeycloakModule` with `keycloakServerUrl`, `keycloakAdminUser`, and `keycloakAdminPassword` configured
- **WHEN** `configureAdminClient()` is invoked
- **THEN** `KeycloakAdminClient` SHALL be bound via `KeycloakAdminClientProvider` as an eager singleton

### Requirement: Keycloak Connector and OpenID Configuration
The module SHALL bind both `KeycloakConnector` and `OpenIdConfigurationProvider` for token validation and OpenID Connect discovery.

#### Scenario: Connector and OpenID provider are bound
- **GIVEN** a `JudoKeycloakModule` is configured
- **WHEN** `configureKeycloakConnector()` is invoked
- **THEN** `KeycloakConnector` SHALL be bound via `KeycloakConnectorProvider` and `OpenIdConfigurationProvider` SHALL be bound via `KeycloakConnectorOpenIdConfigurationProviderProvider`, both as eager singletons

### Requirement: Keycloak User Manager
The module SHALL bind `KeycloakUserManager` for managing users in Keycloak realms.

#### Scenario: User manager is bound
- **GIVEN** a `JudoKeycloakModule` with user manager configuration
- **WHEN** `configureKeycloakUserManager()` is invoked
- **THEN** `KeycloakUserManager` SHALL be bound via `KeycloakUserManagerProvider` as an eager singleton

### Requirement: Realm Synchronization
The module SHALL bind `KeycloakRealmSynchronizer` for synchronizing JUDO model-derived realms with Keycloak.

#### Scenario: Realm synchronizer is bound
- **GIVEN** a `JudoKeycloakModule` is configured
- **WHEN** `configureRealmSyncornizer()` is invoked
- **THEN** `KeycloakRealmSynchronizer` SHALL be bound via `KeycloakRealmSynchronizerProvider` as an eager singleton

### Requirement: Realm Extractor
The module SHALL bind `RealmExtractor` to `PathInfoRealmExtractorProvider` for extracting the Keycloak realm from request path info.

#### Scenario: Realm extractor is bound
- **GIVEN** a `JudoKeycloakModule` is configured
- **WHEN** `configureRealmExtractor()` is invoked
- **THEN** `RealmExtractor` SHALL be bound via `PathInfoRealmExtractorProvider` as an eager singleton

### Requirement: Keycloak Configuration Binding
The module SHALL bind all Keycloak configuration parameters via qualified annotations from `KeycloakConfigurationQualifiers`.

#### Scenario: Keycloak connection parameters are bound
- **GIVEN** a `JudoKeycloakModule` with `keycloakServerUrl("http://keycloak:8080")`, `keycloakAdminUser("admin")`, `keycloakAdminPassword("secret")`
- **WHEN** `configureOptions()` is invoked
- **THEN** `@KeycloakServerUrl` SHALL resolve to `"http://keycloak:8080"`, `@KeycloakAdminUser` to `"admin"`, `@KeycloakAdminPassword` to `"secret"`

### Requirement: Resilience Configuration for Realm Synchronizer
The module SHALL support retry and exponential backoff configuration for the realm synchronizer service.

#### Scenario: Retry configuration is bound
- **GIVEN** a `JudoKeycloakModuleConfiguration` with `keycloakRealmSynchronizerRetryMaxAttempts(5)`, `keycloakRealmSynchronizerRetryExponentialBackoff(true)`, `keycloakRealmSynchronizerRetryWaitDuration(2000L)`
- **WHEN** `configureOptions()` is invoked
- **THEN** `@KeycloakRealmSynchronizerRetryMaxAttempts` SHALL resolve to `5`, `@KeycloakRealmSynchronizerRetryExponentialBackoff` to `true`, `@KeycloakRealmSynchronizerRetryWaitDuration` to `2000L`

### Requirement: Extension Point
The module SHALL provide an empty `configureAdditional()` method that subclasses can override to add custom bindings.

#### Scenario: Subclass adds additional bindings
- **GIVEN** a subclass of `JudoKeycloakModule` that overrides `configureAdditional()`
- **WHEN** `configure()` is invoked
- **THEN** the overridden `configureAdditional()` SHALL be called during module configuration
