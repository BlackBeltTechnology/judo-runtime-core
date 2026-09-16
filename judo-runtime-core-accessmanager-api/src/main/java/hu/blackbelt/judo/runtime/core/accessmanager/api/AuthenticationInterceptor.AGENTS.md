# `AuthenticationInterceptor.java`

Hook interface letting a deployment observe and veto authentication /
authorization of an operation call.

Exports:

- `String getName()` — interceptor identity, no default.
- `default boolean isSuitableForOperation(EOperation operation, String claim, String realm, String client, Map<String, Object> attributes)` — returns `true`, so an interceptor that does not override applies to every operation.
- `default void authenticate(String operationFullyQualifiedName, Map<String, Object> exchange, String claim, String realm, String client, Map<String, Object> attributes)` — empty body.
- `default void success(EOperation operation, SignedIdentifier signedIdentifier, Map<String, Object> exchange, String claim, String realm, String client, Map<String, Object> attributes)` — empty body.

Contracts:

- `authenticate` runs after principal extraction but before the mapped
  principal is loaded. Implementations must not evaluate actor-dependent
  expressions there (for example `getVariables` for actor); the actor is not
  resolved yet.
- `success` runs after authorization succeeded and before the operation itself
  is invoked, and receives the owner `SignedIdentifier` that `authenticate`
  does not get.
- Both hooks are `void` and default to no-op, so refusal is expressed by
  throwing, not by a return value.
