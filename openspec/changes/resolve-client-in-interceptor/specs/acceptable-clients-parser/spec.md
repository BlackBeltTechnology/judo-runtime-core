## ADDED Requirements

### Requirement: AcceptableClientsParser class in security module
The system SHALL provide an `AcceptableClientsParser` utility class in the `hu.blackbelt.judo.runtime.core.security` package of the `judo-runtime-core-security` module.

#### Scenario: Class exists and is accessible
- **WHEN** the `judo-runtime-core-security` module is on the classpath
- **THEN** `hu.blackbelt.judo.runtime.core.security.AcceptableClientsParser` SHALL be available for use by dependent modules

### Requirement: parseAcceptableClients produces actor-to-clients map
The system SHALL provide a static `parseAcceptableClients(String)` method that parses a semicolon-separated configuration string into a `Map<String, Set<String>>` where keys are actor FQNs and values are sets of normalized client names.

#### Scenario: Valid configuration string
- **WHEN** `parseAcceptableClients("com.example.Actor=client-1,client-2;com.example.Other=client-3")` is called
- **THEN** the result SHALL be `{com.example.Actor=[client.1, client.2], com.example.Other=[client.3]}`

#### Scenario: Null or empty input
- **WHEN** `parseAcceptableClients(null)` or `parseAcceptableClients("")` is called
- **THEN** the result SHALL be an empty unmodifiable map

#### Scenario: Dash-to-dot normalization
- **WHEN** a client name contains dashes (e.g., `"my-client-app"`)
- **THEN** dashes SHALL be replaced with dots (e.g., `"my.client.app"`)

#### Scenario: Ambiguous client mapping rejected
- **WHEN** the same client name maps to multiple actor FQNs
- **THEN** an `IllegalArgumentException` SHALL be thrown

#### Scenario: Invalid format rejected
- **WHEN** an entry does not follow the `actorFQN=client1,client2` format
- **THEN** an `IllegalArgumentException` SHALL be thrown

### Requirement: buildClientToActorMap inverts the parsed map
The system SHALL provide a static `buildClientToActorMap(Map<String, Set<String>>)` method that inverts the actor-to-clients map into a `Map<String, String>` where keys are client names and values are actor FQNs.

#### Scenario: Inversion of actor-to-clients map
- **WHEN** `buildClientToActorMap({com.example.Actor=[client.1, client.2]})` is called
- **THEN** the result SHALL be `{client.1=com.example.Actor, client.2=com.example.Actor}`

#### Scenario: Empty input
- **WHEN** `buildClientToActorMap({})` is called
- **THEN** the result SHALL be an empty map
