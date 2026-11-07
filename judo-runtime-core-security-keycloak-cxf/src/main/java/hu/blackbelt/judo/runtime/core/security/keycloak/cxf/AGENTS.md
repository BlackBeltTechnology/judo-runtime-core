# AGENTS.md — `judo-runtime-core-security-keycloak-cxf/src/main/java/hu/blackbelt/judo/runtime/core/security/keycloak/cxf`

| File | Purpose |
| --- | --- |
| `KeycloakLoginInterceptor.java` | CXF `Phase.UNMARSHAL` interceptor; `handleMessage` verifies `Bearer` token with `AdapterTokenVerifier`, installs `KeycloakSecurityContext`. → see `KeycloakLoginInterceptor.java.AGENTS.md` |
| `KeycloakSecurityContext.java` | CXF `SecurityContext` carrying a `JudoPrincipal`. `isUserInRole(String)` matches the role only against the token's `azp` claim (`ISSUED_FOR_KEY`); realm/client roles never match. Callers must set `userPrincipal` before use — null principal NPEs in `isUserInRole`. |