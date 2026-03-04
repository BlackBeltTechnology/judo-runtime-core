# JAX-RS CXF Interceptors Specification

## Purpose
Provides Apache CXF-specific interceptors for request/response exchange ID tracking, fault handling with HTTP status code assignment, and model-driven authorization for JUDO operations.

## Architecture
- **`ExchangeIdDecorator`** -- An inbound CXF interceptor (Phase.RECEIVE) that generates a UUID-based exchange ID for each request and stores it both on the CXF `Exchange` and in the SLF4J MDC for log correlation.
- **`ExchangeIdResponseWriter`** -- An outbound CXF interceptor (Phase.POST_LOGICAL) that writes the exchange ID from the inbound request into an `X-Exchange-Id` response header, including on fault responses.
- **`FaultInterceptor`** -- A CXF interceptor (Phase.PRE_STREAM) that categorizes exceptions by type, assigns appropriate HTTP status codes, and sets the CXF `FaultMode` on the message.
- **`JudoAuthorizingInterceptor`** -- A CXF authorization interceptor that resolves required roles for JUDO operations by inspecting the `@JudoOperation` annotation and the ASM metamodel.

### Class Relationships
```
ExchangeIdDecorator (RECEIVE phase, inbound)
    └── writes exchangeId to Exchange + MDC

ExchangeIdResponseWriter (POST_LOGICAL phase, outbound)
    └── reads exchangeId from Exchange
    └── writes X-Exchange-Id response header

FaultInterceptor (PRE_STREAM phase, outbound)
    └── reads Exception from Exchange
    └── sets FaultMode + HTTP status on response

JudoAuthorizingInterceptor (extends AbstractAuthorizingInInterceptor)
    └── reads @JudoOperation annotation
    └── queries AsmModel for access points and roles
```

### Package
`hu.blackbelt.judo.runtime.core.jaxrs.cxf.interceptors`

## Requirements

### Requirement: UUID-based exchange ID generation per request
`ExchangeIdDecorator` SHALL generate a UUID-based exchange ID for each inbound CXF message and store it on the CXF `Exchange` under the key `"exchangeId"`. If an exchange ID already exists on the exchange, it SHALL reuse it.

#### Scenario: First message on a new exchange
- **GIVEN** an inbound CXF `Message` with no existing exchange ID on its `Exchange`
- **WHEN** `ExchangeIdDecorator.handleMessage()` is invoked
- **THEN** a new UUID string is generated and stored on the `Exchange` under key `"exchangeId"`

#### Scenario: Subsequent message on an existing exchange
- **GIVEN** an inbound CXF `Message` whose `Exchange` already has an `"exchangeId"` value
- **WHEN** `ExchangeIdDecorator.createExchangeId()` is invoked
- **THEN** the existing exchange ID is returned without generating a new one

### Requirement: MDC decoration with exchange ID
`ExchangeIdDecorator` SHALL place the exchange ID into the SLF4J MDC under key `"RequestExchangeId"` to enable log correlation.

#### Scenario: MDC population on request
- **GIVEN** an inbound CXF message
- **WHEN** `ExchangeIdDecorator.handleMessage()` completes
- **THEN** the SLF4J MDC contains the exchange ID under key `"RequestExchangeId"`

### Requirement: Exchange ID propagation to response headers
`ExchangeIdResponseWriter` SHALL write the exchange ID from the CXF `Exchange` into the `X-Exchange-Id` HTTP response header for both normal and fault responses.

#### Scenario: Normal response with exchange ID
- **GIVEN** an outbound CXF response message whose `Exchange` contains an exchange ID
- **WHEN** `ExchangeIdResponseWriter.handleMessage()` is invoked
- **THEN** the response protocol headers include `X-Exchange-Id` with the exchange ID value

#### Scenario: Fault response with exchange ID
- **GIVEN** a CXF fault message whose `Exchange` contains an exchange ID
- **WHEN** `ExchangeIdResponseWriter.handleFault()` is invoked
- **THEN** the response protocol headers include `X-Exchange-Id` with the exchange ID value

### Requirement: Exception-based HTTP status code assignment
`FaultInterceptor` SHALL categorize exceptions and assign HTTP status codes: `InternalServerErrorException` with Jackson cause gets 400, `InternalServerErrorException` with runtime/null cause gets 500, `ClientException` gets its own status code, generic `RuntimeException` gets 500, and checked exceptions get 400.

#### Scenario: Jackson deserialization failure
- **GIVEN** an `InternalServerErrorException` whose cause is a `com.fasterxml.jackson` exception
- **WHEN** `FaultInterceptor.handleFault()` is invoked
- **THEN** the HTTP response status is set to 400 and `FaultMode` is `CHECKED_APPLICATION_FAULT`

#### Scenario: ClientException with custom status code
- **GIVEN** a `ClientException` with a status code of 403
- **WHEN** `FaultInterceptor.handleFault()` is invoked
- **THEN** the HTTP response status is set to 403 and `FaultMode` is `CHECKED_APPLICATION_FAULT`

#### Scenario: Generic RuntimeException
- **GIVEN** a generic `RuntimeException` on the exchange
- **WHEN** `FaultInterceptor.handleFault()` is invoked
- **THEN** the HTTP response status is set to 500 and `FaultMode` is `RUNTIME_FAULT`

### Requirement: Fault mode passthrough for already-handled faults
`FaultInterceptor` SHALL not reassign the HTTP status code when a `FaultMode` is already present on the message, allowing previously handled faults to pass through.

#### Scenario: Fault already handled by another interceptor
- **GIVEN** a CXF message that already has a `FaultMode` set
- **WHEN** `FaultInterceptor.handleFault()` is invoked
- **THEN** the interceptor returns immediately without modifying the response status

### Requirement: ASM model-driven operation authorization
`JudoAuthorizingInterceptor` SHALL resolve the expected roles for a JAX-RS method by inspecting the `@JudoOperation` annotation, looking up the corresponding `EOperation` in the `AsmModel`, and returning the fully qualified names of non-public access points as required roles. Public actors (those without a realm) result in an empty role list.

#### Scenario: Operation exposed by a public (realm-less) actor
- **GIVEN** a method annotated with `@JudoOperation` whose ASM operation is exposed by an access point with no `realm` attribute
- **WHEN** `JudoAuthorizingInterceptor.getExpectedRoles()` is invoked
- **THEN** an empty list is returned, allowing unauthenticated access

#### Scenario: Operation exposed by a secured actor with a realm
- **GIVEN** a method annotated with `@JudoOperation` whose ASM operation is exposed by an access point with a non-empty `realm` attribute
- **WHEN** `JudoAuthorizingInterceptor.getExpectedRoles()` is invoked
- **THEN** a list containing the fully qualified name of the access point is returned

#### Scenario: Method without JudoOperation annotation
- **GIVEN** a method without a `@JudoOperation` annotation
- **WHEN** `JudoAuthorizingInterceptor.getExpectedRoles()` is invoked
- **THEN** an empty list is returned
