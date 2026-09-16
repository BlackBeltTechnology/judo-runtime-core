# AGENTS.md — `judo-runtime-core-guice/src/main/java/hu/blackbelt/judo/runtime/core/guice/accessmanager`

Guice providers for the access-manager layer: the default `AccessManager` and the authentication interceptor provider.

| File | Purpose |
| --- | --- |
| `DefaultAccessManagerProvider.java` | Guice `Provider<AccessManager>`; `@Inject` constructor takes the bound `AsmModel`, `get()` returns `DefaultAccessManager.builder().asmModel(asmModel).build()`. Contract: `AsmModel` binding must exist; every `get()` yields a fresh `DefaultAccessManager`. |
| `DefaultAuthenticationInterceptorProviderProvider.java` | Guice `Provider<AuthenticationInterceptorProvider>`; `get()` returns an anonymous `AuthenticationInterceptorProvider` whose `getAuthenticationInterceptors()` hands back a mutable `ArrayList<AuthenticationInterceptor>` seeded empty. Contract: callers register interceptors by adding to the returned collection before dispatch. |