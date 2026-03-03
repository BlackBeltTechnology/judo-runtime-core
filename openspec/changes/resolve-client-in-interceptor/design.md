## Context

Currently, `DefaultActorResolver` in the `dispatcher` module owns the `parseAcceptableClients()` logic and the client-to-actor resolution via `resolveActorType()`. The `KeycloakLoginInterceptor` in `security-keycloak-cxf` passes the raw Keycloak client ID (after dash-to-dot conversion) into `JudoPrincipal.client`. Downstream consumers then need to resolve this to an actor FQN.

The `security-keycloak-cxf` module already depends on `judo-runtime-core-security`. The `dispatcher` module also depends on `judo-runtime-core-security`. This shared dependency makes `judo-runtime-core-security` the natural home for a shared parsing utility.

## Goals / Non-Goals

**Goals:**
- Move `parseAcceptableClients()` to a shared `AcceptableClientsParser` class in `judo-runtime-core-security`
- Add `buildClientToActorMap()` to produce a `Map<String, String>` (client → actor FQN) for interceptor use
- Resolve `principal.client` to actor FQN in `KeycloakLoginInterceptor` when an acceptable clients mapping exists
- Maintain full backward compatibility when no acceptable clients are configured

**Non-Goals:**
- Changing the `JudoPrincipal` class or adding new fields
- Modifying `DefaultActorResolver.resolveActorType()` logic (it continues to work as-is)
- Requiring acceptable clients configuration (it remains optional)

## Decisions

### 1. Utility class placement: `judo-runtime-core-security`

**Decision:** Create `AcceptableClientsParser` in `hu.blackbelt.judo.runtime.core.security` package.

**Rationale:** Both `security-keycloak-cxf` and `dispatcher` already depend on this module. The parsing logic is pure Java (no external dependencies), so it introduces zero new transitive dependencies. Client-to-actor resolution is semantically a security concern.

**Alternative considered:** Placing in `judo-runtime-core` (core module) — rejected because the security module is a more semantically appropriate location for authentication-related utilities.

### 2. Map inversion via `buildClientToActorMap()`

**Decision:** Add a static method that inverts `Map<String, Set<String>>` (actor → clients) to `Map<String, String>` (client → actor) for O(1) lookup by client ID.

**Rationale:** The interceptor needs to look up by client ID, not by actor FQN. The inverted map is the natural structure for this. The ambiguity validation in `parseAcceptableClients()` already guarantees each client maps to exactly one actor, so the inversion is safe.

### 3. Injection into `KeycloakLoginInterceptor`

**Decision:** Add an optional `Map<String, String> clientToActorMap` parameter to the `KeycloakLoginInterceptor` builder. When present and the client is found in the map, use the mapped actor FQN. Otherwise, fall through to existing `convertClientToActorName()` behavior.

**Rationale:** Making it optional preserves backward compatibility. The interceptor doesn't need the raw parsed map — only the inverted client→actor map.

### 4. `DefaultActorResolver` delegates parsing

**Decision:** `DefaultActorResolver` calls `AcceptableClientsParser.parseAcceptableClients()` instead of its own static method. The local method is removed.

**Rationale:** Single source of truth. Existing tests for parsing move to the security module (or test the shared utility).

## Risks / Trade-offs

- [Risk] Existing tests for `parseAcceptableClients()` are in `dispatcher` module → Move or duplicate tests to cover the utility in the security module. Dispatcher tests can delegate to shared utility tests.
- [Risk] OSGi bundle exports may need updating for `judo-runtime-core-security` → Verify `Export-Package` includes the new class.
- [Trade-off] The interceptor now has an optional dependency on configuration — but this is injected, not discovered, so it's clean.
