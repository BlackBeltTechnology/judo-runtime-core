# `BehaviourAuthorizer.java`

Abstract base every behaviour authorizer in this package extends. Defines the
authorization contract and owns the single place where the ASM `permissions`
annotation is interpreted.

## Public surface

- `abstract boolean isSuitableForOperation(EOperation operation)` — the subclass
  claims exactly one `AsmUtils.OperationBehaviour` (two, where validate and mutate
  share rights).
- `abstract void authorize(String actorFqName, Collection<String> publicActors,
  SignedIdentifier signedIdentifier, EOperation operation)` — returns normally on
  grant, throws on denial. There is no boolean result.
- `enum CRUDFlag { CREATE("create"), UPDATE("update"), DELETE("delete") }` — the
  enum constant carries `permissionName`, the detail key read out of the annotation.

## Shared check — `checkCRUDFlag(AsmModel, ENamedElement, CRUDFlag...)`

Package-private, so only siblings in this package may call it.

1. Reads the `permissions` extension annotation off `element` via
   `AsmUtils.getExtensionAnnotationByName(element, "permissions", false)`.
2. Passes when the annotation exists and at least one requested flag's
   `permissionName` parses as `true` — varargs are OR-ed, never AND-ed.
3. On failure, when `element` is an `EOperation` whose owner resolves through
   `getOwnerOfOperationWithDefaultBehaviour` and whose behaviour is **not**
   `GET_REFERENCE_RANGE`, it retries the same flag test against the owner's
   `permissions` annotation; a missing owner annotation is `IllegalStateException`.
4. Otherwise it logs and throws `AccessDeniedException` carrying a
   `ValidationResult` with `code("PERMISSION_DENIED")`, `Level.ERROR`,
   `location(element.getName())`, and details `MISSING_PRIVILEGES` (the requested
   flags) plus `MODEL_ELEMENT` (reference/operation FQ name, or plain name).

## Contracts

- Denial is signalled by exception only; a subclass that returns quietly grants
  access. `RefreshAuthorizer` relies on this deliberately.
- Subclasses must not re-read the `permissions` annotation themselves — the
  owner-fallback and the `AccessDeniedException` payload shape live only here.
- `@Slf4j` logging is informational; the thrown `ValidationResult` is the contract
  consumed by callers, so its `code` and detail keys are API.
