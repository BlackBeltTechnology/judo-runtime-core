# AGENTS.md — `judo-runtime-core-accessmanager/src/main/java/hu/blackbelt/judo/runtime/core/accessmanager/behaviours`

One authorizer per ASM `OperationBehaviour`. Each subclasses `BehaviourAuthorizer`, claims its
behaviour via `isSuitableForOperation`, and either passes or throws from `authorize`. Adding a new
default behaviour without adding an authorizer here leaves that operation unguarded.

| File | Purpose |
| --- | --- |
| `AddReferenceAuthorizer.java` | Guards `ADD_REFERENCE` operations. Lombok `@Builder` over `@NonNull AsmModel asmModel`. `authorize` requires `signedIdentifier.getProducedBy()` non-null — otherwise `SecurityException("Unable to check permissions")` — then demands `CRUDFlag.UPDATE` on the producing `ETypedElement`, not on the target reference. |
| `BehaviourAuthorizer.java` | Abstract base declaring `isSuitableForOperation` / `authorize`, the shared `checkCRUDFlag` annotation test, and `enum CRUDFlag`. Denial is exception-only. → see `BehaviourAuthorizer.java.AGENTS.md` |
| `CreateInstanceAuthorizer.java` | Guards `CREATE_INSTANCE` and `VALIDATE_CREATE`. Resolves the operation owner via `getOwnerOfOperationWithDefaultBehaviour`, requiring `CRUDFlag.CREATE` plus an `exposedBy` annotation matching `actorFqName` or a public actor. When owner is not `annotatedAsTrue(owner, "access")` it additionally requires `CRUDFlag.UPDATE` on the producer. Missing owner is `IllegalStateException`, not denial. |
| `DeleteInstanceAuthorizer.java` | Guards `DELETE_INSTANCE`. Requires `signedIdentifier.getProducedBy()` non-null and `CRUDFlag.DELETE` on that producer element; delete rights ride on the producing relation, never on the instance type. |
| `GetInputRangeAuthorizer.java` | Guards `GET_INPUT_RANGE`. Checks only when `signedIdentifier` is non-null — an unbound (null-identifier) range call is intentionally allowed. When bound, the operation owner must carry an `exposedBy` value in `publicActors` or equal to `actorFqName`, else `SecurityException("Permission denied")`. No CRUD flag is consulted. |
| `GetReferenceRangeAuthorizer.java` | Guards `GET_REFERENCE_RANGE`. Null `signedIdentifier` passes unchecked. Otherwise the producer must exist and hold `CRUDFlag.CREATE` **or** `CRUDFlag.UPDATE` — the only authorizer accepting either flag. |
| `GetTemplateAuthorizer.java` | Guards `GET_TEMPLATE`. `authorize` only asserts the operation has an owner with a default behaviour (`IllegalStateException` otherwise); permission checking is an open `TODO: JNG-2180`. Treat template retrieval as unauthorized-by-design today. |
| `ListAuthorizer.java` | Guards `LIST`. Resolves the operation owner and requires an `exposedBy` annotation value in `publicActors` or equal to `actorFqName`, else `SecurityException("Permission denied")`. Exposure-only check — no `CRUDFlag` is read, so listing does not imply read permissions on members. |
| `RefreshAuthorizer.java` | Guards `REFRESH`. `authorize` is deliberately empty: every refresh is permitted once the identifier is signed. Holds `asmModel` only to satisfy the base shape. |
| `RemoveReferenceAuthorizer.java` | Guards `REMOVE_REFERENCE`. Requires a non-null producer and `CRUDFlag.UPDATE` on it — removing a link is modelled as an update of the producing element, not a delete. |
| `SetReferenceAuthorizer.java` | Guards `SET_REFERENCE`. Requires a non-null producer and `CRUDFlag.UPDATE` on it; identical rights to add/remove/unset, so a single-valued set cannot be granted separately. |
| `UnsetReferenceAuthorizer.java` | Guards `UNSET_REFERENCE`. Requires a non-null producer and `CRUDFlag.UPDATE` on it; clearing a reference needs update rights, not delete rights. |
| `UpdateInstanceAuthorizer.java` | Guards `UPDATE_INSTANCE` and `VALIDATE_UPDATE` — validation shares the write permission of the real mutation. Requires a non-null producer and `CRUDFlag.UPDATE` on it. |
