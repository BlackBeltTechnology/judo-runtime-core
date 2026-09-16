# `KeycloakRealmSynchronizer.java`

Pushes realms and clients from the keycloak metamodel into the server.
`synchronizeAllRealms()` wraps a Resilience4j `Retry` (`RetryUtil.createRetryRegistry`) around `KeycloakAdminClient`, creating/updating only dirty realms/clients; runs async on `customExecutor` when `asyncServiceCall` and fires `registerIdentityManagerReady` on success.
`AccessType` enum (`PUBLIC`, `CONFIDENTIAL`, `BEARER_ONLY`); defaults `systemDefaultAccessType=CONFIDENTIAL`, `humanDefaultSystemAccessType=BEARER_ONLY`, `retryMaxAttempts=1000`.