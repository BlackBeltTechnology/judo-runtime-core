# AGENTS.md — `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/util`

Support classes a test reaches for directly: environment-variable substitution around a
test body, reflection-based dependency injection for classes that would normally be wired
by OSGi, and a mutable interceptor provider.

| File | Purpose |
| --- | --- |
| `EnvironmentVariableMocker.java` | Takes over `java.lang.ProcessEnvironment` with `Mockito.mockStatic` so `System.getenv` reads a pushed map. Exports `initMocked()`, `deinitMocked()`, `connect(Map)`, `pop()`. Intercepts only `getenv`, `environment`, `toEnvironmentBlock`; every other call and an empty `REPLACEMENT_ENV` stack fall through to the real method. Maps are held in a `Stack`, all mutation guarded by a `sync` monitor. |
| `EnvironmentVariables.java` | Named env-var set applied around a test body; `SingularTestResource` + `NameValuePairSetter`, exports `and`/`set`/`remove`/`getVariables()`/`doSetup`/`doTeardown` push/pop via `EnvironmentVariableMocker`. → see `EnvironmentVariables.java.AGENTS.md` |
| `ReferenceInjector.java` | Injects Guice bindings into objects OSGi would normally wire; exports `injectReferences(Object, Injector)`, `createAndInject(Class<T>, Injector)`, fills known non-static non-final fields + `set*` setters. → see `ReferenceInjector.java.AGENTS.md` |
| `TestOperationCallInterceptorProvider.java` | Mutable `OperationCallInterceptorProvider` for tests; exports `addInterceptor`/`removeInterceptor`/`clearInterceptors`/`getInterceptors`/`size`/`isEmpty`/`getCallOperationInterceptors`. → see `TestOperationCallInterceptorProvider.java.AGENTS.md` |
