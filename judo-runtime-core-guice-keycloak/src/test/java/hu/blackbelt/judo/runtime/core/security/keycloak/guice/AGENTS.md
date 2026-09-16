# AGENTS.md — `judo-runtime-core-guice-keycloak/src/test/java/hu/blackbelt/judo/runtime/core/security/keycloak/guice`

Integration test for the Keycloak Guice module wiring against the rest of the runtime.

| File | Purpose |
| --- | --- |
| `JudeKeycloakModuleTest.java` | Boots Keycloak wiring via `Guice.createInjector(Modules.combine(...))` over `JudoDefaultModule`, `JudoHsqldbModule`, `JudoJettyModule`, `JudoCxfModule`, `JudoKeycloakModule`; `@BeforeEach` runs `keycloakRealmSynchronizer.synchronizeAllRealms()`. `testIdentityManagerBecomesReadyWithin5Seconds` polls `userManager.isIdentityManagerReady()` via `Awaitility` with 5 s cap. |