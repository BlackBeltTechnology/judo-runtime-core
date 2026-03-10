## ADDED Requirements

### Requirement: Platform parameter for acceptable clients
The system SHALL support a `JUDO_PLATFORM_ACCEPTABLE_CLIENTS` environment variable (or OSGi `acceptableClients` config property) that defines a key-value map of actor type FQNs to additional accepted Keycloak client IDs. The format SHALL be `ActorFQN=client1,client2;OtherActorFQN=client3`. Client values SHALL use normalized form (dots, not dashes).

#### Scenario: Parameter configured via environment variable
- **WHEN** `JUDO_PLATFORM_ACCEPTABLE_CLIENTS` is set to `MyModel.UserActor=frontend.app,mobile.app;MyModel.AdminActor=admin.tool`
- **THEN** `DefaultActorResolver` SHALL receive a parsed `Map<String, Set<String>>` with two entries: `MyModel.UserActor → {frontend.app, mobile.app}` and `MyModel.AdminActor → {admin.tool}`

#### Scenario: Parameter not configured
- **WHEN** `JUDO_PLATFORM_ACCEPTABLE_CLIENTS` is not set or empty
- **THEN** `DefaultActorResolver` SHALL receive an empty map and existing behavior SHALL be unchanged

### Requirement: Fallback client resolution in authenticateByPrincipal
The `authenticateByPrincipal` method SHALL first attempt to resolve `principal.getClient()` as an actor type FQN via `asmUtils.resolve()`. If that fails, it SHALL look up `principal.getClient()` in the acceptable clients map values to find the corresponding actor type FQN, then resolve that FQN to an EClass.

#### Scenario: Client matches actor FQN directly (existing behavior)
- **WHEN** `principal.getClient()` is `MyModel.UserActor` and `asmUtils.resolve("MyModel.UserActor")` returns an EClass
- **THEN** the system SHALL use that EClass as the actor type (existing behavior, no map lookup)

#### Scenario: Client is a whitelisted non-actor client
- **WHEN** `principal.getClient()` is `frontend.app` and `asmUtils.resolve("frontend.app")` returns empty
- **AND** acceptable clients map contains `MyModel.UserActor → {frontend.app}`
- **THEN** the system SHALL resolve `MyModel.UserActor` as the actor type and proceed with authentication

#### Scenario: Client is not recognized
- **WHEN** `principal.getClient()` is `unknown.client` and `asmUtils.resolve("unknown.client")` returns empty
- **AND** `unknown.client` does not appear in any acceptable clients map entry
- **THEN** the system SHALL throw `IllegalStateException("Unsupported client")`

### Requirement: Per-actor-type scoping prevents privilege escalation
Each acceptable client SHALL be mapped to exactly one actor type. A token issued for a whitelisted client SHALL only resolve to the actor type it is mapped to.

#### Scenario: Client mapped to UserActor cannot access AdminActor
- **WHEN** `principal.getClient()` is `frontend.app`
- **AND** acceptable clients map contains `MyModel.UserActor → {frontend.app}` but NOT `MyModel.AdminActor → {frontend.app}`
- **THEN** the system SHALL resolve the actor type as `MyModel.UserActor` only, never `MyModel.AdminActor`

### Requirement: Ambiguous client mapping rejected at startup
If the same client ID appears in multiple actor type entries in the acceptable clients configuration, the system SHALL reject the configuration at `DefaultActorResolver` construction time.

#### Scenario: Duplicate client across actor types
- **WHEN** acceptable clients is configured as `MyModel.UserActor=shared.app;MyModel.AdminActor=shared.app`
- **THEN** `DefaultActorResolver` construction SHALL throw `IllegalArgumentException` indicating the ambiguous mapping for `shared.app`

#### Scenario: Same client within one actor type (valid)
- **WHEN** acceptable clients is configured as `MyModel.UserActor=app.one,app.two`
- **THEN** `DefaultActorResolver` construction SHALL succeed with both clients mapped to `MyModel.UserActor`

### Requirement: Parameter propagation through platform activator
The `acceptableClients` property SHALL be added to the appropriate platform activator's PIDS map and propagated to `DefaultActorResolver` via the existing OSGi/Guice/Spring injection pattern.

#### Scenario: Property propagated through PIDS map
- **WHEN** the platform activator receives `acceptableClients` property
- **THEN** it SHALL be included in the properties passed to the component that constructs `DefaultActorResolver`
