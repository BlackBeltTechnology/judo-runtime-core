# AGENTS.md — `judo-runtime-core-accessmanager/src/main/java/hu/blackbelt/judo/runtime/core/accessmanager`

Default `AccessManager` implementation: decides from ASM `exposedBy`/`realm`
annotations whether the caller's actor may invoke an operation, then delegates
behaviour-specific checks to the authorizers in `.behaviours`.

| File | Purpose |
|---|---|
| `DefaultAccessManager.java` | ASM-annotation-driven `AccessManager`. Lombok `@Builder` takes `@NonNull asmModel` and optional `authenticationInterceptorProvider`; constructor precomputes `publicActors` and the 12 `BehaviourAuthorizer` instances. → see `DefaultAccessManager.AGENTS.md` |
