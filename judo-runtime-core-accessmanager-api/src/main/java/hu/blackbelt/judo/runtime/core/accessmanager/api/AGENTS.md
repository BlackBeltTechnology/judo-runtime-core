# AGENTS.md — `judo-runtime-core-accessmanager-api/src/main/java/hu/blackbelt/judo/runtime/core/accessmanager/api`

Contract-only package: the SPI a runtime dispatcher calls to decide whether a
bound operation invocation is allowed, plus the signed-identifier value passed
along with it.

| File | Purpose |
|---|---|
| `AccessManager.java` | Declares authorization entry point for operation calls. Interface `AccessManager` exports one method `authorizeOperation(EOperation operation, SignedIdentifier signedIdentifier, Map<String, Object> exchange)`. Method returns `void`: implementations must signal denial by throwing, since a normal return is read by callers as "call permitted". |
| `AuthenticationInterceptor.java` | Hook interface letting a deployment observe and veto authentication of an operation call; exports `getName()` plus no-op defaults `isSuitableForOperation`, `authenticate`, `success`. → see `AuthenticationInterceptor.AGENTS.md` |
| `AuthenticationInterceptorProvider.java` | Supplies the interceptor chain to whoever runs authorization. Interface exports default `getAuthenticationInterceptors()` returning a new empty `ArrayList`, so a provider that does not override contributes nothing instead of failing the call. |
| `SignedIdentifier.java` | Immutable value identifying the instance a bound operation was called on. Lombok `@Getter`/`@Builder` class exposes `identifier` (`@NonNull`), `producedBy` (`ETypedElement`), `entityType`, `version`, `immutable`; all fields `final`, no setters. Builder throws `NullPointerException` when `identifier` is left unset. |
