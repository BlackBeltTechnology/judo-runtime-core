## Context

`DefaultActorResolver.authenticateByPrincipal()` resolves the actor type by calling `asmUtils.resolve(principal.getClient())`, where `client` is the Keycloak token's `azp` claim (normalized: dashes→dots via `convertClientToActorName`). This requires the `azp` value to exactly match an actor type FQN in the ASM model.

The method is called from 4 places:
- `DefaultActorResolver.authenticateActor()` — has exchange with `operationFullyQualifiedName`
- `ExportCall`, `ListCall`, `GetPrincipalCall` — call directly with only the `JudoPrincipal`

The `JudoPrincipal` lives in `judo-dispatcher-api` (separate repo) and carries: `name`, `realm`, `client`, `attributes`.

## Goals / Non-Goals

**Goals:**
- Allow additional Keycloak client IDs to authenticate via a platform-level configuration
- Map each additional client to a specific actor type (key-value)
- No changes to `JudoPrincipal`, `ActorResolver` interface, or caller signatures
- Backward compatible: empty config = existing behavior

**Non-Goals:**
- Wildcard or regex matching for client IDs
- Per-realm scoping of acceptable clients (the actor FQN key implicitly scopes to the correct realm)
- Changes to `judo-dispatcher-api`

## Decisions

### 1. Configuration format: semicolon-separated key-value pairs

**Format:** `ActorFQN=client1,client2;OtherActor=client3,client4`

**Rationale:** Fits the existing platform pattern where parameters are strings passed through OSGi `@AttributeDefinition`. A single string property is simplest to propagate through the PIDS map. Parsing is straightforward and happens once at `DefaultActorResolver` construction.

**Alternative considered:** Separate property per actor type (e.g., `acceptableClients.MyModel.User=app1,app2`). Rejected because the PIDS map in platform activators requires explicit property names — dynamic keys don't fit the pattern.

### 2. Resolution logic: fallback after existing behavior

```
1. Try asmUtils.resolve(principal.getClient())     → existing path
2. If empty, search acceptableClients map values    → new fallback
3. Use matched key as actor type FQN
4. If no match → "Unsupported client"
```

**Rationale:** Existing behavior is preserved as primary path. The map is only consulted when the client doesn't resolve as an actor FQN. No behavior change for existing deployments.

### 3. Client values stored in normalized form (dots, not dashes)

**Rationale:** `KeycloakLoginInterceptor.convertClientToActorName()` normalizes `azp` by replacing dashes with dots. The comparison in `authenticateByPrincipal` uses `principal.getClient()` which is already normalized. Config values must match this form.

**Example:** Keycloak client `my-frontend-app` → config value `my.frontend.app`

### 4. Ambiguous mapping rejected at construction time

If the same client ID appears in multiple actor type entries, `DefaultActorResolver` throws `IllegalArgumentException` at construction. This prevents non-deterministic behavior at runtime.

**Rationale:** Fail-fast is safer than silent unpredictable resolution. An admin misconfiguration should surface immediately at startup, not as an intermittent auth bug.

### 5. Injection point: `DefaultActorResolver` constructor

Add `Map<String, Set<String>> acceptableClients` parameter to the `@Builder` constructor. Parsed from the string format by the caller (Guice module / Spring autoconfiguration / platform activator).

**Rationale:** Keeps parsing outside the resolver. Each integration framework (Guice, Spring, OSGi) can parse the string in its own configuration layer.

## Risks / Trade-offs

- **Misconfiguration risk** → Mitigated by startup validation (ambiguous mapping check) and logging of parsed acceptable clients at INFO level
- **Normalized form confusion** → Config must use dots not dashes; documented in parameter description and logged at startup
- **No hot-reload** → Changing `acceptableClients` requires restart. Acceptable since actor type configuration is also static. Could be enhanced later if needed.
- **Large map scanning** → Linear scan of map values on each auth for non-FQN clients. Acceptable: the map is expected to be small (handful of entries). Could pre-compute reverse map if needed.
