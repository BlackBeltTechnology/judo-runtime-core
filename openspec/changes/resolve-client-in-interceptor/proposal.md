## Why

The `acceptableClients` configuration maps Keycloak client IDs to actor FQNs, but this resolution currently happens late in `DefaultActorResolver` (dispatcher module). The `KeycloakLoginInterceptor` (security-keycloak-cxf module) already has the client ID at authentication time but passes it through without resolving. By resolving client → actor early in the interceptor, `JudoPrincipal.client` carries the actor FQN from the start, making downstream processing simpler and the resolution logic reusable across modules.

## What Changes

- Extract `parseAcceptableClients()` from `DefaultActorResolver` into a new `AcceptableClientsParser` utility class in `judo-runtime-core-security` module
- Add `buildClientToActorMap()` to invert the map (client → actor FQN) for O(1) lookup
- Inject the client-to-actor map into `KeycloakLoginInterceptor`
- Resolve `principal.client` to actor FQN in the interceptor when a mapping exists; fall through to `convertClientToActorName()` result when no mapping exists (backward compatible)
- `DefaultActorResolver` delegates parsing to the shared `AcceptableClientsParser`

## Capabilities

### New Capabilities
- `acceptable-clients-parser`: Shared utility for parsing acceptable clients configuration and resolving client IDs to actor FQNs, located in the security module
- `interceptor-client-resolution`: Early client-to-actor resolution in `KeycloakLoginInterceptor` using the acceptable clients map

### Modified Capabilities

## Impact

- **Modules touched**: `judo-runtime-core-security`, `judo-runtime-core-security-keycloak-cxf`, `judo-runtime-core-dispatcher`, `judo-runtime-core-guice`, `judo-runtime-core-guice-keycloak`, `judo-runtime-core-spring`
- **APIs**: `KeycloakLoginInterceptor` gains a new dependency (client-to-actor map injection)
- **Backward compatible**: No config changes required. Without `acceptableClients` config, behavior is identical to today
- **`DefaultActorResolver`**: Parsing delegated to shared utility; `resolveActorType()` still works but in the mapped case receives an already-resolved actor FQN
