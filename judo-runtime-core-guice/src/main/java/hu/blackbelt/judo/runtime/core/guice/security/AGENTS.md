# AGENTS.md — `judo-runtime-core-guice/src/main/java/hu/blackbelt/judo/runtime/core/guice/security`

Guice `Provider<T>` factories for security realm/password-policy components.

| File | Purpose |
| --- | --- |
| `NoPasswordPolicyProvider.java` | Guice `Provider<PasswordPolicy>`. `get()` returns `new NoPasswordPolicy()` — accepts any password, enforces nothing. Use when the app binds no real `PasswordPolicy`. |
| `PathInfoRealmExtractorProvider.java` | Guice `Provider<RealmExtractor>`. `get()` constructs `PathInfoRealmExtractor` over `JudoModelLoader.getAsmModel()` — realm taken from request path segments against the asm model. Requires `JudoModelLoader` bound. |