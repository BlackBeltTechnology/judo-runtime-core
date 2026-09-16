# AGENTS.md — `judo-runtime-core-guice-keycloak/src/main/java/hu/blackbelt/judo/runtime/core/security/keycloak/guice`

Guice wiring for Keycloak security integration: module, configuration value object, and binding qualifiers.

| File | Purpose |
| --- | --- |
| `JudoKeycloakModule.java` | Guice `AbstractModule` wiring Keycloak security: login interceptor, password policy, admin client, connector, user manager, realm synchronizer, realm extractor. → see `JudoKeycloakModule.java.AGENTS.md` |
| `JudoKeycloakModuleConfiguration.java` | `@Builder`-backed value object feeding `JudoKeycloakModule`; static `DEFAULT` holds defaults incl. server url `http://localhost:8080/auth`, `keycloakAdminUser=admin`, `keycloakAdminPassword=judo`, client access types `CONFIDENTIAL`/`BEARER_ONLY`, retry `maxAttempts=1000`/`waitDuration=1000L`, `keycloakSecurityPasswordPolicyType=NO_PASSWORD`. |
| `KeycloakConfigurationQualifiers.java` | Grouping class owning 21 `@BindingAnnotation` qualifiers for Keycloak config: `KeycloakServerUrl`, `KeycloakPublicUrl`, `KeycloakAdminUser`, `KeycloakAdminPassword`, `KeycloakClientSecret`, realm-synchronizer and user-manager families, `KeycloakIdentityManagerIsReady`, `KeycloakSecurityPasswordPolicyType`; each `@Qualifier` with RUNTIME retention bounded to FIELD/PARAMETER/METHOD targets. |