# `DefaultAccessManager.java`

ASM-annotation-driven implementation of `AccessManager`. Answers "may this
principal call this operation" from `exposedBy` / `realm` extension annotations,
then delegates behaviour-specific checks to the `.behaviours` authorizers.

Exports:

- Lombok `@Builder` constructor `DefaultAccessManager(@NonNull AsmModel asmModel, AuthenticationInterceptorProvider authenticationInterceptorProvider)`.
- `authorizeOperation(EOperation, SignedIdentifier, Map<String, Object> exchange)` (`@Override`).

Construction-time state:

- `publicActors` — FQ names of every actor `EClass` whose `realm` annotation is
  absent or empty. Computed once in the constructor, so a later `asmModel`
  mutation is not observed.
- `authorizers` — fixed list built by `setupAuthorizers`: `ListAuthorizer`,
  `CreateInstanceAuthorizer`, `UpdateInstanceAuthorizer`,
  `DeleteInstanceAuthorizer`, `RefreshAuthorizer`, `SetReferenceAuthorizer`,
  `UnsetReferenceAuthorizer`, `AddReferenceAuthorizer`,
  `RemoveReferenceAuthorizer`, `GetReferenceRangeAuthorizer`,
  `GetInputRangeAuthorizer`, `GetTemplateAuthorizer`. A new behaviour authorizer
  is only reached once added to this list.

`authorizeOperation` contract, in order:

1. `exchange.get(Dispatcher.PRINCIPAL_KEY)`, when non-null, must be a
   `JudoPrincipal`; anything else throws `IllegalStateException("Unsupported principal")`.
2. `GET_PRINCIPAL` behaviour without a principal throws
   `AuthenticationRequiredException` with code `INVALID_TOKEN`.
3. Operation not exposed to a public actor nor to the token's actor, and not
   `GET_METADATA`: throws `AccessDeniedException` (`ACCESS_DENIED`) when an
   actor is present, otherwise `AuthenticationRequiredException`
   (`AUTHENTICATION_REQUIRED`).
4. Bound-operation instance whose `signedIdentifier.getProducedBy()` is not
   `exposedBy` a permitted actor throws `AccessDeniedException`
   (`ACCESS_DENIED_FOR_INSTANCE_OF_BOUND_OPERATION`).
5. With a provider and a principal, every suitable `AuthenticationInterceptor`
   gets `success(...)` — after the checks above, before the authorizers run.
6. Each authorizer reporting `isSuitableForOperation` gets
   `authorize(actorFqName, publicActors, signedIdentifier, operation)`.

Every rejection carries a `ValidationResult` at `Level.ERROR`; a normal return
means the call is permitted.
