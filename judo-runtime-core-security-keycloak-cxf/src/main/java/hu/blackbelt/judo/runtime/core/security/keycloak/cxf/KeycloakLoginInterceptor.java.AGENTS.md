# `KeycloakLoginInterceptor.java`

CXF `Phase.UNMARSHAL` interceptor authenticating JAX-RS calls.
`handleMessage(Message)` verifies the `Authorization: Bearer` token with `AdapterTokenVerifier` against a cached per-realm `KeycloakDeployment`, maps claims onto actor attributes, then installs CXF `SecurityContext` (`KeycloakSecurityContext` over `JudoPrincipal`).
`@Builder` requires `realmExtractor`, `asmModel`, `openIdConfigurationProvider`, `transformationTraceService`.
Expired token throws `AuthenticationRequiredException` (`ACCESS_TOKEN_EXPIRED`), invalid token `AuthenticationException`; missing token leaves the call unauthenticated with no `SecurityContext` — downstream must not assume authentication occurred.