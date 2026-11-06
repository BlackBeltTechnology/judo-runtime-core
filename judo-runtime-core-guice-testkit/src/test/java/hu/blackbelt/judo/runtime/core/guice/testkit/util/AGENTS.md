# AGENTS.md — `judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util`

| File | Purpose |
| --- | --- |
| `EnvironmentVariableMockerTest.java` | Tests `EnvironmentVariableMocker` (replaces `System.getenv`): `initMocked`/`deinitMocked` idempotent, stack ops `connect`/`pop`/`remove`, null-value filtering, preserve/override of system variables. Ordered via `@TestMethodOrder`. |
| `EnvironmentVariablesTest.java` | Tests `EnvironmentVariables`: constructors (empty/single/multi-pair), immutable/mutable ops, set/remove, execution context with env vars, error handling. Needs `EnvironmentVariableMocker.initMocked()` around suite or `getEnv` returns real env. |
| `ReferenceInjectorTest.java` | Demonstrates `ReferenceInjector` wiring OSGi `@Reference` into custom implementations: field (`SampleInterceptorWithFields`) and setter (`SampleInterceptorWithSetters`) injection of example `UserDao`/`PartnerDao` pairs. |
| `TestOperationCallInterceptorProviderTest.java` | Unit tests `TestOperationCallInterceptorProvider`: `addInterceptor` preserves order, `removeInterceptor`, `size`/`isEmpty`, `getInterceptors`/`getCallOperationInterceptors` mirror contents. |