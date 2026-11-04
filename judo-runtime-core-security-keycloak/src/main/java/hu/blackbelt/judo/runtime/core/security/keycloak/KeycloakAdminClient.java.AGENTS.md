# `KeycloakAdminClient.java`

REST client over the Keycloak Admin API.
`getUsersOfRealm`, `getUserOfRealm`, `getClientsOfRealm`, `getListOfRealms` read realm state; `createOrUpdateClient`, `createOrUpdateRealm`, `createUserRestCall`, `updateUserRestCall`, `deleteUserRestCall` mutate it; all routed through `keycloakConnector.getAdminClient()`.
`@Builder` requires a non-null `KeycloakConnector`; `getUserOfRealm(realm, null)` throws `IllegalArgumentException` via `checkArgument`.