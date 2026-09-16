# AGENTS.md — `judo-runtime-core-dispatcher/src/main/java/hu/blackbelt/judo/runtime/core/dispatcher/security`

Security service-provider interfaces the dispatcher calls out to: resolving the authenticated
actor instance behind a token, and signing/verifying transfer-object identifiers exposed to
clients. Implementations live outside this package.

| File | Purpose |
| --- | --- |
| `ActorResolver.java` | Interface that maps an authenticated token to a persisted actor instance. Declares `authenticateActor(Map exchange)`, `authenticateByPrincipal(JudoPrincipal)` → `Optional<Payload>`, `getActorByClaims(EClass actorType, Map claims)` → `Payload`. Implementations load the actor from the database; an unmapped claim set yields empty, never a synthetic actor. |
| `IdentifierSigner.java` | Interface that signs exposed instance identifiers so clients cannot forge them. Exports `SIGNED_IDENTIFIER_KEY = "__signedIdentifier"`, `signIdentifiers(ETypedElement, Map payload, boolean immutable)` mutating payload in place, and `extractSignedIdentifier(EClass, Map)` → `Optional<SignedIdentifier>`. Outbound payloads must be signed before return; unsigned or tampered input yields empty. |
