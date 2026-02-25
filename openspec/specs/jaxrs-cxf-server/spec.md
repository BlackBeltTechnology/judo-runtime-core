# JAX-RS CXF Server Providers Specification

## Purpose
Provides a CXF-server-specific copy of the core JAX-RS provider components (exception mappers, payload writer, and content-type filter) for deployment within CXF server configurations, mirroring the functionality of the base `judo-runtime-core-jaxrs` module.

## Architecture
- **`ClientExceptionMapper`** -- Maps `ClientException` instances to HTTP responses with the appropriate status code and JSON error details.
- **`RuntimeExceptionMapper`** -- Catches all `RuntimeException` types and maps them to structured JSON error responses with differentiated status codes for `ClientErrorException`, `WebApplicationException`, `BusinessException`, and generic runtime exceptions.
- **`PayloadMessageBodyWriter`** -- A `MessageBodyWriter` that serializes `Payload`/`PayloadImpl` objects to JSON using a configurable Jackson `ObjectMapper`.
- **`SetDefaultContentTypePreMatchContainerRequestFilter`** -- A pre-matching `ContainerRequestFilter` that injects a default `Content-Type` header on body-bearing requests that lack one.

### Package
`hu.blackbelt.judo.runtime.core.jaxrs.providers` (same package as the base jaxrs module, deployed in the CXF server context)

## Requirements

### Requirement: Client exception mapping to HTTP response
`ClientExceptionMapper` SHALL map a `ClientException` to an HTTP response using the exception's `statusCode` (defaulting to 400 if null), with the exception's `details` as a JSON entity body.

#### Scenario: ClientException with explicit status code
- **GIVEN** a `ClientException` with `statusCode` of 409 and a details payload
- **WHEN** `ClientExceptionMapper.toResponse()` is invoked
- **THEN** the response has HTTP status 409, content type `application/json`, and the body contains the exception details

#### Scenario: ClientException with null status code
- **GIVEN** a `ClientException` with `statusCode` of null
- **WHEN** `ClientExceptionMapper.toResponse()` is invoked
- **THEN** the response has HTTP status 400 (Bad Request)

### Requirement: Runtime exception mapping with categorized status codes
`RuntimeExceptionMapper` SHALL map `RuntimeException` subtypes to differentiated HTTP status codes: `ClientErrorException` returns its own response, `WebApplicationException` with Jackson cause returns 400 with `INVALID_JSON` code, `BusinessException` returns 422 with fault type header and filtered details, and generic `RuntimeException` returns 500 with `INTERNAL_SERVER_ERROR` code.

#### Scenario: BusinessException mapping with X-Fault header
- **GIVEN** a `BusinessException` with type `"ValidationFault"` and detail entries including some with `_`-prefixed keys
- **WHEN** `RuntimeExceptionMapper.toResponse()` is invoked
- **THEN** the response has HTTP status 422, an `X-Fault` header set to `"ValidationFault"`, and a JSON body with details filtered to exclude `_`-prefixed keys and null values

#### Scenario: WebApplicationException with Jackson cause
- **GIVEN** a `WebApplicationException` whose cause class name starts with `com.fasterxml.jackson`
- **WHEN** `RuntimeExceptionMapper.toResponse()` is invoked
- **THEN** the response has HTTP status 400 and JSON body contains `"code": "INVALID_JSON"`

#### Scenario: ClientErrorException passthrough
- **GIVEN** a `ClientErrorException` with an existing response
- **WHEN** `RuntimeExceptionMapper.toResponse()` is invoked
- **THEN** the original response from the `ClientErrorException` is returned directly

### Requirement: Configurable stack trace and business cause inclusion
`RuntimeExceptionMapper` SHALL include the exception stack trace in the response when `returnRuntimeExceptions` is `true` (except for `BusinessException`), and SHALL include the `BusinessException` cause when `includeBusinessCause` is `true`.

#### Scenario: Stack trace included for non-business exceptions
- **GIVEN** `RuntimeExceptionMapper` with `returnRuntimeExceptions` set to `true`
- **WHEN** a generic `RuntimeException` is mapped
- **THEN** the response JSON `details` object contains an `exception` key with the full stack trace

#### Scenario: Business cause included when configured
- **GIVEN** `RuntimeExceptionMapper` with `includeBusinessCause` set to `true` and a `BusinessException` with a non-null cause
- **WHEN** the exception is mapped
- **THEN** the response JSON contains a `"cause"` field

### Requirement: Payload serialization via MessageBodyWriter
`PayloadMessageBodyWriter` SHALL serialize `Payload` and `PayloadImpl` instances to JSON using the configured `ObjectMapper`.

#### Scenario: Writing a PayloadImpl to the output stream
- **GIVEN** a `PayloadMessageBodyWriter` with a configured `ObjectMapper`
- **WHEN** `writeTo()` is called with a `PayloadImpl` instance
- **THEN** the payload is serialized as JSON to the entity output stream

#### Scenario: Writeability check for supported types
- **GIVEN** a `PayloadMessageBodyWriter`
- **WHEN** `isWriteable()` is called with `Payload.class` or `PayloadImpl.class`
- **THEN** the method returns `true`

### Requirement: Default content-type injection for body-bearing requests
`SetDefaultContentTypePreMatchContainerRequestFilter` SHALL set `Content-Type` to `application/json` on POST, PUT, and PATCH requests that have no `Content-Type` header, and SHALL skip GET and DELETE requests.

#### Scenario: POST request missing Content-Type
- **GIVEN** an incoming POST request with no `Content-Type` header
- **WHEN** `SetDefaultContentTypePreMatchContainerRequestFilter.filter()` processes the request
- **THEN** the `Content-Type` header is set to `application/json`

#### Scenario: GET request without Content-Type
- **GIVEN** an incoming GET request with no `Content-Type` header
- **WHEN** `SetDefaultContentTypePreMatchContainerRequestFilter.filter()` processes the request
- **THEN** the `Content-Type` header remains unset
