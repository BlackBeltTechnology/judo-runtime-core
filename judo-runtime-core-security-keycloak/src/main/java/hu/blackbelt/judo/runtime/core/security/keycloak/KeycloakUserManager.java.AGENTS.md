# `KeycloakUserManager.java`

Implements `UserManager<String>` over the Keycloak Admin API.
Exports `getUser`, `getAllUsers`, `createUser`, `updateUser`, `deleteUser`, `getPrincipalAttributeMapping`, `getManagedActorOfPrincipal`, `getUsername`, plus claim constants `KEYCLOAK_ID`, `KEYCLOAK_USERNAME_CLAIM`, `KEYCLOAK_EMAIL_CLAIM`, `KEYCLOAK_CREDENTIALS_CLAIM`, `KEYCLOAK_REQUIRED_ACTIONS_CLAIM`, `KEYCLOAK_PASSWORD_TYPE`.
`@Builder` requires a non-null `defaultPasswordPolicy`; `createUser` applies it and writes a `password` credential only when the policy yields a value.