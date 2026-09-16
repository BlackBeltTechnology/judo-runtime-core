# `KeycloakConnector.java`

`OpenIdConfigurationProvider` for a live Keycloak server.
`getClient(realm)`/`getAdminClient()` build CXF `WebClient`s (admin uses a password-grant `access_token` from `admin-cli`); `getOpenIdConfiguration(EClass)` rewrites `serverUrl` to `externalUrl` when set; `ping()` throws `IllegalStateException` unless the server answers 200/302; `getClientId(EClass)` renders the actor FQN with `.`→`-`.
Caches per-actor-type realm and openid configs; actor types without a `realm` annotation yield `null` from config lookups.