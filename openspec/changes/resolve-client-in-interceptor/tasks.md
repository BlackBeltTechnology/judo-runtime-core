## 1. Create AcceptableClientsParser in security module

- [x] 1.1 Create `AcceptableClientsParser` class in `hu.blackbelt.judo.runtime.core.security` package with `parseAcceptableClients(String)` method (move logic from `DefaultActorResolver`)
- [x] 1.2 Add `buildClientToActorMap(Map<String, Set<String>>)` method that inverts the map to `Map<String, String>` (client → actor FQN)
- [x] 1.3 Verify OSGi `Export-Package` in `judo-runtime-core-security/pom.xml` includes the package

## 2. Update DefaultActorResolver to delegate parsing

- [x] 2.1 Replace `DefaultActorResolver.parseAcceptableClients()` with a call to `AcceptableClientsParser.parseAcceptableClients()`
- [x] 2.2 Update `DefaultActorResolverAcceptableClientsTest` to test via `AcceptableClientsParser` (or keep testing through `DefaultActorResolver` if it delegates correctly)

## 3. Update KeycloakLoginInterceptor to resolve client

- [x] 3.1 Add optional `Map<String, String> clientToActorMap` parameter to `KeycloakLoginInterceptor` builder constructor
- [x] 3.2 In `handleMessage()`, after `convertClientToActorName()`, look up the normalized client in `clientToActorMap`. If found, use the mapped actor FQN as `principal.client`; otherwise use the normalized client as today

## 4. Update DI configurations

- [x] 4.1 Update `KeycloakLoginInterceptorProvider` (Guice) to inject `acceptableClients` string, parse it via `AcceptableClientsParser`, and pass the inverted map to the interceptor builder
- [x] 4.2 Update `JudoKeycloakModule` to bind the `acceptableClients` configuration for the provider if needed
- [x] 4.3 Verify Spring configuration — if Spring uses Keycloak interceptor, update accordingly (currently Keycloak is Guice-only)
