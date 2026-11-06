# AGENTS.md — `judo-runtime-core-security-keycloak/src/main/java/hu/blackbelt/judo/runtime/core/security/keycloak`

| File | Purpose |
| --- | --- |
| `KeycloakAdminClient.java` | REST client over the Keycloak Admin API; realm reads/mutations routed through `keycloakConnector.getAdminClient()`. → see `KeycloakAdminClient.java.AGENTS.md` |
| `KeycloakConnector.java` | `OpenIdConfigurationProvider` for a live Keycloak server; `getClient(realm)`/`getAdminClient()`/`ping()`. → see `KeycloakConnector.java.AGENTS.md` |
| `KeycloakRealmSynchronizer.java` | Pushes realms/clients from the keycloak metamodel into the server; `synchronizeAllRealms()` wraps `RetryUtil.createRetryRegistry` `Retry`. → see `KeycloakRealmSynchronizer.java.AGENTS.md` |
| `KeycloakUserManager.java` | `UserManager<String>` over the Keycloak Admin API; exports `getUser`/`createUser`/`deleteUser` + `KEYCLOAK_*` claim constants. → see `KeycloakUserManager.java.AGENTS.md` |
| `RetryUtil.java` | Resilience4j factory. `createRetryRegistry(int maxAttempts, long waitDuration, boolean exponentialBackoff)` retries `NotFoundException`, `ConnectException`, `ProcessingException`, `IllegalStateException` with `failAfterMaxAttempts(true)`; `registerLogEventHandlers(Retry)` wires error/retry/success/ignored events to SLF4J levels. On other exception types no retry happens. |
| `SameEmailPasswordPolicy.java` | `PasswordPolicy<String>` returning the user's email as default password. `apply(Map)` reads `KeycloakUserManager.KEYCLOAK_EMAIL_CLAIM` and returns empty `Optional` when the claim is absent — callers must handle users without email. |
| `SameUsernamePasswordPolicy.java` | `PasswordPolicy<String>` whose `apply(Map)` returns `KeycloakUserManager.KEYCLOAK_USERNAME_CLAIM` as default password; empty `Optional` when the claim is absent. |