## ADDED Requirements

### Requirement: KeycloakLoginInterceptor accepts optional client-to-actor map
The `KeycloakLoginInterceptor` SHALL accept an optional `Map<String, String>` (client → actor FQN) via its builder constructor. When not provided, behavior SHALL be identical to the current implementation.

#### Scenario: Map not provided
- **WHEN** `KeycloakLoginInterceptor` is constructed without a `clientToActorMap`
- **THEN** `principal.client` SHALL be set to `convertClientToActorName(accessToken.getIssuedFor())` as today

#### Scenario: Map provided but client not found
- **WHEN** `KeycloakLoginInterceptor` is constructed with a `clientToActorMap` that does not contain the current client
- **THEN** `principal.client` SHALL be set to `convertClientToActorName(accessToken.getIssuedFor())` (fallback)

### Requirement: Interceptor resolves client to actor FQN when mapping exists
When a `clientToActorMap` is provided and the normalized client name is found in the map, the interceptor SHALL set `JudoPrincipal.client` to the mapped actor FQN.

#### Scenario: Client found in map
- **WHEN** the Keycloak token's `issuedFor` is `"frontend-app"` and the `clientToActorMap` contains `{"frontend.app": "com.example.UserActor"}`
- **THEN** `principal.client` SHALL be `"com.example.UserActor"`

#### Scenario: Resolution happens before principal creation
- **WHEN** a request arrives with a valid Bearer token
- **THEN** the client-to-actor resolution SHALL happen in the interceptor before `JudoPrincipal` is built, so downstream consumers receive the resolved actor FQN

### Requirement: DefaultActorResolver delegates parsing to AcceptableClientsParser
`DefaultActorResolver` SHALL use `AcceptableClientsParser.parseAcceptableClients()` from the security module instead of its own local parsing method.

#### Scenario: Parsing delegated
- **WHEN** `DefaultActorResolver` is constructed with an `acceptableClients` configuration string
- **THEN** it SHALL delegate to `AcceptableClientsParser.parseAcceptableClients()` for parsing

### Requirement: DI frameworks inject client-to-actor map into interceptor
The Guice and Spring configurations SHALL parse the `acceptableClients` string and inject the resulting inverted `Map<String, String>` into `KeycloakLoginInterceptor`.

#### Scenario: Guice configuration
- **WHEN** `actorResolverAcceptableClients` is configured in the Guice module
- **THEN** the parsed and inverted map SHALL be injected into `KeycloakLoginInterceptor`

#### Scenario: Spring configuration
- **WHEN** `judo.actorResolver.acceptableClients` property is set
- **THEN** the parsed and inverted map SHALL be injected into `KeycloakLoginInterceptor`

#### Scenario: No configuration provided
- **WHEN** no acceptable clients configuration exists
- **THEN** `KeycloakLoginInterceptor` SHALL receive null or empty map, and behavior SHALL be unchanged
