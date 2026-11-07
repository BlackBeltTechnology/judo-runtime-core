# `UserManagedWrappedDao.java`

`DAO` decorator forwarding every operation to a `delegatee`, gated by a
`UserManager<String>` write-through so principal-managed actors are written to
the user store instead of the raw DAO.

Exports:

- Lombok `@Builder` constructor `UserManagedWrappedDao(@NonNull DAO delegatee, UserManager<String> userManager, @NonNull Context context, @NonNull IdentifierProvider identifierProvider, Boolean userManagerEnabled)`.
- `setUserManager(UserManager<String>)` (volatile setter, settable at runtime).
- Full `DAO` surface delegated to `delegatee` (static features, CRUD, references, navigation, search).

Contracts a caller can violate:

- Write ops (`create/createAll/update/updateAll/delete/deleteAll`, referenced-instance and navigation-instance writes) throw `IllegalArgumentException("User manager is not started yet")` while `userManagerEnabled` and `userManager == null`. Caller must set manager before first write.
- User-store sync skips when context `ROLLBACK` key is `Boolean.TRUE` (rollback path); otherwise principal writes go through `userManager.getManagedActorOfPrincipal` / `getUsername` / `createUser` and throw `checkArgument`/`checkState` on missing loaded user or username.
- `convertPrincipalToActor(EClass, Payload)` rewrites a principal payload to actor shape via `getPrincipalAttributeMapping` (dropping null-mapped keys) before user-store writes.