## Why

The `authenticateByPrincipal` method in `DefaultActorResolver` currently requires that the JWT token's `azp` (authorized party) claim — after dash-to-dot conversion — exactly matches an actor type FQN in the ASM model. This prevents legitimate use cases where multiple Keycloak clients (e.g., mobile apps, admin tools, third-party frontends) need to authenticate against the same backend actors. A platform-level system parameter is needed to whitelist additional Keycloak client IDs that should be accepted.

## What Changes

- Add an `acceptableClients` system parameter in `judo-platform` as a `JUDO_PLATFORM_` environment variable / OSGi configuration property, using key-value format: `ActorTypeFQN=client1,client2;OtherActor=client3`
  - Configured via environment variable: `JUDO_PLATFORM_ACCEPTABLE_CLIENTS=MyModel.UserActor=frontend.app,mobile.app;MyModel.AdminActor=admin.tool`
  - Or via OSGi config property `acceptableClients` in the dispatcher/security activator
  - Propagated through the existing PIDS pattern in `DispatcherServiceActivator` (or the appropriate security activator)
- Inject this parsed map (`Map<String, Set<String>>`) into `DefaultActorResolver`
- Modify `authenticateByPrincipal` to:
  1. First try existing behavior: resolve `principal.getClient()` as an actor FQN via `asmUtils.resolve()`
  2. If that fails, look up `principal.getClient()` in the `acceptableClients` map values to find the corresponding actor type FQN
  3. Resolve the actor type from the matched key
  4. If neither resolves, throw "Unsupported client"
- No signature changes to `ActorResolver` interface or `JudoPrincipal`
- No changes needed for callers (`ExportCall`, `ListCall`, `GetPrincipalCall`) — the resolution is self-contained in `authenticateByPrincipal`
- Client values in the config must use the **normalized** form (dots, not dashes) to match `convertClientToActorName` output from `KeycloakLoginInterceptor`

### Security Constraints

- **Token verification is unchanged**: Keycloak's `AdapterTokenVerifier.verifyToken()` still validates signature, expiry, and audience before any client check
- **Realm boundary intact**: Keycloak deployment is per-realm; tokens from one realm cannot authenticate against another
- **Per-actor-type scoping**: Each acceptable client is mapped to a specific actor type, preventing privilege escalation (a token for `frontend-app` mapped to `UserActor` cannot access `AdminActor` data)
- **Ambiguous mapping rejection**: If the same client ID appears in multiple actor type mappings, the configuration must be rejected at startup to prevent non-deterministic actor resolution
- **Existing behavior preserved**: Actor-FQN-based client resolution (current behavior) always takes precedence; the map is only consulted as a fallback

## Capabilities

### New Capabilities
- `acceptable-clients`: Platform-level key-value map of actor types to additional acceptable Keycloak client IDs, with fallback actor type resolution in `DefaultActorResolver`

### Modified Capabilities

## Impact

- **judo-platform**: New system parameter `acceptableClients` in the dispatcher/security activator, propagated via the existing OSGi PIDS pattern
- **judo-runtime-core-dispatcher**: `DefaultActorResolver` gains a `Map<String, Set<String>> acceptableClients` field; `authenticateByPrincipal` logic changes to check the map as fallback
- **Backward compatible**: Empty map = current behavior unchanged; no interface or signature changes
