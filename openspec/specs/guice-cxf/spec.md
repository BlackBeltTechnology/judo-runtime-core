# Guice CXF Module Specification

## Purpose
Provides a Google Guice module (`JudoCxfModule`) that configures Apache CXF JAX-RS server integration for JUDO runtime, including REST endpoint setup, CORS filtering, JSON serialization, exception mapping, exchange ID tracking, fault interception, and authorization.

## Architecture
- **JudoCxfModule** (`hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.JudoCxfModule`): `AbstractModule` that configures the CXF JAX-RS server, providers, and interceptors using Guice `Multibinder` for extensibility.
- **JudoCxfModuleConfiguration** (`hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.JudoCxfModuleConfiguration`): Lombok `@Builder` configuration for CXF and CORS settings.
- **CxfConfigurations** (`hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfConfigurations`): Binding annotations for CXF configuration (server URL, path, CORS settings, logging, metrics, exception handling).
- **CxfQualifiers** (`hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfQualifiers`): Qualifiers for multi-binding sets (`@Providers`, `@InInterceptors`, `@OutInterceptors`, `@FaultInterceptors`).

### Provider Classes
| Provider | Purpose |
|----------|---------|
| `CxfJaxrsServerProvider` | Creates and configures the CXF JAX-RS `Server` with all providers and interceptors |
| `ClientExceptionMapperProvider` | Maps client exceptions to HTTP responses |
| `RuntimeExceptionMapperProvider` | Maps runtime exceptions to HTTP responses |
| `CrossOriginResourceSharingFilterProvider` | Configures CORS filter for cross-origin requests |
| `SetDefaultContentTypePreMatchContainerRequestFilterProvider` | Sets default content type on incoming requests |
| `PayloadMessageBodyWriterProvider` | Custom message body writer for JUDO `Payload` objects |
| `JacksonJaxbJsonProviderProvider` | Configures Jackson JSON serialization for JAX-RS |
| `ExtendedObjectMapperProvider` | Provides extended Jackson `ObjectMapper` |
| `ISO8601DateParamHandlerProvider` | Handles ISO 8601 date parameter parsing |
| `ExchangeIdDecoratorProvider` | In-interceptor that decorates requests with exchange IDs |
| `ExchangeIdResponseWriterProviderOut` | Out-interceptor that writes exchange IDs to responses |
| `ExchangeIdResponseWriterProviderFault` | Fault-interceptor that writes exchange IDs to fault responses |
| `FaultInterceptorProvider` | Fault-interceptor for error handling |
| `JudoAuthorizingInterceptorProvider` | In-interceptor for authorization checks |

## Requirements

### Requirement: CXF JAX-RS Server Initialization
The module SHALL create and start a CXF JAX-RS server as an eager singleton, assembling all registered providers and interceptors.

#### Scenario: Server is created with all providers
- **GIVEN** a `JudoCxfModule` installed in the Guice injector
- **WHEN** the injector is created
- **THEN** `CxfJaxrsServerProvider.ServerHolder` SHALL be bound as an eager singleton and the CXF server SHALL be initialized with all multi-bound providers and interceptors

### Requirement: Multibinder-Based Extension Points
The module SHALL use Guice `Multibinder` to allow extensible registration of JAX-RS providers, in-interceptors, out-interceptors, and fault-interceptors via `CxfQualifiers`.

#### Scenario: Providers are registered via Multibinder
- **GIVEN** a `JudoCxfModule` with default configuration
- **WHEN** `configure()` completes
- **THEN** the `@CxfQualifiers.Providers` set SHALL contain bindings for `ClientExceptionMapper`, `PayloadMessageBodyWriter`, `CrossOriginResourceSharingFilter`, `SetDefaultContentTypePreMatchContainerRequestFilter`, `JacksonJaxbJsonProvider`, and `ISO8601DateParamHandler`

#### Scenario: Interceptors are registered via Multibinder
- **GIVEN** a `JudoCxfModule` with default configuration
- **WHEN** `configure()` completes
- **THEN** the `@CxfQualifiers.InInterceptors` set SHALL contain the `JudoAuthorizingInterceptor`, and `@CxfQualifiers.FaultInterceptors` SHALL contain the `FaultInterceptor`

### Requirement: Exchange ID Tracking
The module SHALL conditionally register exchange ID interceptors on in, out, and fault chains based on the `exchangeIdInterceptors` configuration flag.

#### Scenario: Exchange ID interceptors enabled
- **GIVEN** `JudoCxfModuleConfiguration` with `exchangeIdInterceptors` set to `true`
- **WHEN** `configure()` is invoked
- **THEN** `ExchangeIdDecoratorProvider` SHALL be added to in-interceptors, `ExchangeIdResponseWriterProviderOut` to out-interceptors, and `ExchangeIdResponseWriterProviderFault` to fault-interceptors

#### Scenario: Exchange ID interceptors disabled
- **GIVEN** `JudoCxfModuleConfiguration` with `exchangeIdInterceptors` set to `false`
- **WHEN** `configure()` is invoked
- **THEN** no exchange ID interceptors SHALL be registered

### Requirement: CORS Configuration
The module SHALL bind CORS configuration parameters via qualified annotations, supporting allow origin, credentials, headers, expose headers, max age, preflight error status, and block-if-unauthorized settings.

#### Scenario: CORS filter is registered with configuration
- **GIVEN** a `JudoCxfModule` with `corsAllowOrigin("*")` and `corsAllowCredentials(true)`
- **WHEN** the module is configured
- **THEN** `CrossOriginResourceSharingFilterProvider` SHALL be added to the providers set, and `@CxfCorsAllowOrigin` SHALL resolve to `"*"` and `@CxfCorsAllowCredentials` to `true`

### Requirement: CXF Configuration Binding
The module SHALL bind all CXF operational parameters via qualified annotations from `CxfConfigurations`.

#### Scenario: Server URL and path are bound
- **GIVEN** a `JudoCxfModule` with `cxfJaxRsServerUrl` and `cxfJaxRsServerPath` configured
- **WHEN** `configureOptions()` is invoked
- **THEN** `@CxfJaxRsServerUrl` and `@CxfJaxRsServerPath` SHALL be injectable with the configured values

### Requirement: JSON Serialization
The module SHALL register Jackson JSON serialization support for JAX-RS via `JacksonJaxbJsonProviderProvider`.

#### Scenario: Jackson provider is registered
- **GIVEN** a `JudoCxfModule` with default configuration
- **WHEN** the module is configured
- **THEN** `JacksonJaxbJsonProviderProvider` SHALL be added to the providers multibinder set
