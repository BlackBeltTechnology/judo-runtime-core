# Security Keycloak CXF Specification

## Purpose
The security-keycloak-cxf module integrates Keycloak OAuth2/OpenID Connect authentication with the Apache CXF JAX-RS framework, providing a CXF interceptor that extracts and verifies Bearer tokens from HTTP requests and establishes the security context for downstream processing.

## Architecture

### Key Classes
- `KeycloakLoginInterceptor` -- a CXF `AbstractPhaseInterceptor<Message>` running in the `UNMARSHAL` phase. It extracts the actor type via `RealmExtractor`, obtains the Keycloak realm from ASM annotations, verifies the Bearer token using Keycloak's `AdapterTokenVerifier`, maps token claims to actor attributes via `TransformationTraceService` attribute bindings, and sets the CXF `SecurityContext` with a `JudoPrincipal`.
- `KeycloakSecurityContext` -- implements CXF `SecurityContext`, holding the `JudoPrincipal` and implementing `isUserInRole` by comparing the role to the `azp` (authorized party) claim from the access token.

### Integration Points
- Depends on `RealmExtractor` (from security module) for actor type extraction.
- Depends on `OpenIdConfigurationProvider` (from security module) for the Keycloak server URL.
- Depends on `TransformationTraceService` for mapping Keycloak `AttributeBinding` to ASM actor attributes.
- Uses Keycloak Java adapter (`KeycloakDeploymentBuilder`, `AdapterTokenVerifier`) for token verification.

## Requirements

### Requirement: Bearer Token Extraction and Verification
`KeycloakLoginInterceptor` SHALL extract the Bearer token from the HTTP Authorization header, verify it against the Keycloak deployment, and establish a security context.

#### Scenario: Successful token verification
- **GIVEN** an HTTP request with a valid `Authorization: Bearer <token>` header targeting a realm-protected actor endpoint
- **WHEN** `handleMessage` is called
- **THEN** the token is verified via `AdapterTokenVerifier.verifyToken`, token claims are mapped to actor attributes, and a `KeycloakSecurityContext` with a `JudoPrincipal` is set on the CXF message

#### Scenario: Expired token
- **GIVEN** an HTTP request with an expired Bearer token
- **WHEN** `handleMessage` is called
- **THEN** a `TokenNotActiveException` is caught and an `AuthenticationRequiredException` is thrown with code `ACCESS_TOKEN_EXPIRED`

#### Scenario: Invalid or tampered token
- **GIVEN** an HTTP request with an invalid Bearer token
- **WHEN** `handleMessage` is called
- **THEN** a `VerificationException` or `JWSInputException` is caught and an `AuthenticationException` is thrown with message "Authentication failed"

#### Scenario: Missing authorization header
- **GIVEN** an HTTP request without an Authorization header targeting a realm-protected endpoint
- **WHEN** `handleMessage` is called
- **THEN** no security context is set and processing continues (allowing public access paths)

#### Scenario: Non-bearer authorization scheme
- **GIVEN** an HTTP request with `Authorization: Basic <credentials>`
- **WHEN** `handleMessage` is called
- **THEN** a warning is logged and no security context is set

### Requirement: Actor Type Resolution from Request
`KeycloakLoginInterceptor` SHALL use `RealmExtractor` to determine the actor type and its associated Keycloak realm from the HTTP request.

#### Scenario: Known actor type with realm
- **GIVEN** a request path matching a known actor type that has a `realm` annotation
- **WHEN** `handleMessage` is called
- **THEN** the realm is extracted and used to build/retrieve the `KeycloakDeployment` for token verification

#### Scenario: No actor type matched
- **GIVEN** a request path that does not match any actor type
- **WHEN** `handleMessage` is called
- **THEN** no token verification is performed and processing continues

### Requirement: Token Claim to Actor Attribute Mapping
`KeycloakLoginInterceptor` SHALL map Keycloak access token claims to ASM actor attributes using `TransformationTraceService` attribute bindings.

#### Scenario: Map preferred_username to USERNAME claim
- **GIVEN** an actor type with an attribute annotated with claim=USERNAME, and the transformation trace maps it to Keycloak attribute binding "username"
- **WHEN** a valid token is verified
- **THEN** the `preferred_username` claim from the token is mapped to the actor's USERNAME attribute in the `JudoPrincipal.attributes`

### Requirement: Keycloak Deployment Caching
`KeycloakLoginInterceptor` SHALL cache `KeycloakDeployment` instances per realm to avoid repeated configuration building.

#### Scenario: Reuse cached deployment
- **GIVEN** a `KeycloakDeployment` has been created for realm "myRealm"
- **WHEN** a subsequent request for the same realm arrives
- **THEN** the cached `KeycloakDeployment` is reused from `keycloakDeploymentMap`

### Requirement: CXF Security Context Role Checking
`KeycloakSecurityContext` SHALL determine role membership by comparing the role to the `azp` (authorized party) claim.

#### Scenario: User in role matching azp
- **GIVEN** a `KeycloakSecurityContext` with a `JudoPrincipal` whose attributes contain `azp=my-client`
- **WHEN** `isUserInRole("my-client")` is called
- **THEN** true is returned

#### Scenario: User not in role
- **GIVEN** a `KeycloakSecurityContext` with `azp=my-client`
- **WHEN** `isUserInRole("other-client")` is called
- **THEN** false is returned

### Requirement: Client Name to Actor Name Conversion
`KeycloakLoginInterceptor` SHALL convert Keycloak client IDs (hyphen-separated) to actor fully qualified names (dot-separated).

#### Scenario: Convert client name
- **GIVEN** a Keycloak token with `issuedFor` value `com-example-MyActor`
- **WHEN** the principal is created
- **THEN** the client is set to `com.example.MyActor`
