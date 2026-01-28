# JUDO Guice CXF Module

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-guice-cxf
- **Version**: ${project.version}

## Overview

This module provides Google Guice integration for Apache CXF JAX-RS, enabling REST API server capabilities with CORS support, Jackson JSON serialization, exception handling, and CXF interceptors.

## Main Components

### JudoCxfModule

The primary Guice module that configures the CXF JAX-RS server.

```java
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.JudoCxfModule;

// Basic usage with defaults
Injector injector = Guice.createInjector(
    JudoCxfModule.builder().build()
);

// Production configuration
Injector injector = Guice.createInjector(
    JudoCxfModule.builder()
        .cxfJaxRsServerUrl("http://localhost:8080")
        .cxfJaxRsServerPath("api")
        .corsAllowOrigin("https://myapp.com")
        .corsAllowCredentials(true)
        .cxfMetricsEnabled(true)
        .cxfLoggingEnabled(false)
        .build()
);
```

### Configuration Options

#### Server Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `cxfJaxRsServerUrl` | String | `"http://localhost"` | Base server URL |
| `cxfJaxRsServerPath` | String | `"api"` | API path prefix |
| `cxfSkipDefaultJsonProviderRegistration` | Boolean | `false` | Skip default JSON provider |
| `cxfWadlServiceDescriptionAvailable` | Boolean | `true` | Enable WADL generation |
| `cxfMetricsEnabled` | Boolean | `true` | Enable metrics collection |
| `cxfLoggingEnabled` | Boolean | `true` | Enable request/response logging |
| `cxfLogException` | Boolean | `true` | Log exceptions |
| `cxfReturnRuntimeExceptions` | Boolean | `true` | Return runtime exceptions to client |
| `cxfIncludeBusinessCause` | Boolean | `true` | Include business exception causes |
| `cxfDefaultRequestContentType` | String | `"application/json"` | Default content type |
| `exchangeIdInterceptors` | Boolean | `true` | Enable exchange ID tracking |

#### CORS Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `corsAllowOrigin` | String | `"*"` | Allowed origins |
| `corsAllowCredentials` | Boolean | `true` | Allow credentials |
| `corsAllowHeaders` | String | (see below) | Allowed headers |
| `corsExposeHeaders` | String | `"X-Exchange-Id,X-Fault,X-Judo-Count"` | Exposed headers |
| `corsMaxAge` | Integer | `-1` | Preflight cache duration |
| `corsPrefligthErrorStatus` | Integer | `400` | Preflight error status |
| `corsBlockIfUnauthorized` | Boolean | `false` | Block unauthorized requests |

Default `corsAllowHeaders`: `Content-Type,Origin,Accept,Authorization,X-Judo-SignedIdentifier,X-Judo-CountRecords`

### Provided Bindings

The module configures these components via Guice multibinders:

#### Providers (JAX-RS)
- `CrossOriginResourceSharingFilter` - CORS handling
- `ClientExceptionMapper` - Client exception mapping
- `PayloadMessageBodyWriter` - Payload serialization
- `JacksonJaxbJsonProvider` - JSON serialization
- `ISO8601DateParamHandler` - Date parameter parsing
- `SetDefaultContentTypePreMatchContainerRequestFilter` - Content-type defaults

#### Interceptors (CXF)
- **In Interceptors**: `ExchangeIdDecorator`, `JudoAuthorizingInterceptor`
- **Out Interceptors**: `ExchangeIdResponseWriter`
- **Fault Interceptors**: `FaultInterceptor`, `ExchangeIdResponseWriter`

## Maven Dependency

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-guice-cxf</artifactId>
    <version>${project.version}</version>
</dependency>
```

## Usage with Jetty

Typically used with the Jetty module:

```java
Injector injector = Guice.createInjector(
    JudoJettyModule.builder()
        .jettyServerPort(8080)
        .build(),
    JudoCxfModule.builder()
        .cxfJaxRsServerUrl("http://localhost:8080")
        .cxfJaxRsServerPath("api/v1")
        .corsAllowOrigin("https://frontend.example.com")
        .build()
);
```

## Adding Custom Providers

Use Guice multibinders to add custom JAX-RS providers:

```java
public class CustomCxfModule extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder<Object> providers = Multibinder.newSetBinder(
            binder(), Object.class, CxfQualifiers.Providers.class);
        providers.addBinding().to(MyCustomProvider.class);
    }
}
```

## Adding Custom Interceptors

```java
Multibinder<Interceptor> inInterceptors = Multibinder.newSetBinder(
    binder(), Interceptor.class, CxfQualifiers.InInterceptors.class);
inInterceptors.addBinding().to(MyInInterceptor.class);

Multibinder<Interceptor> outInterceptors = Multibinder.newSetBinder(
    binder(), Interceptor.class, CxfQualifiers.OutInterceptors.class);
outInterceptors.addBinding().to(MyOutInterceptor.class);
```

## Configuration Qualifiers

```java
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfConfigurations;

@Inject
@CxfConfigurations.CxfJaxRsServerUrl
private String serverUrl;

@Inject
@CxfConfigurations.CxfCorsAllowOrigin
private String allowedOrigins;
```
