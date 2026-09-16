# AGENTS.md — `judo-runtime-core-guice-testkit/src/main/java/hu/blackbelt/judo/runtime/core/guice/testkit/examples`

Example test classes showing how to drive the JUDO testkit: `@JudoTest` annotation modes and fixture extensions.

| File | Purpose |
| --- | --- |
| `AnnotationBasedTest.java` | Shows `@JudoTest` annotation on tests with injected `JudoRuntimeFixture`, incl. `TransactionHandling` modes and dialect/modelName options. → see `AnnotationBasedTest.java.AGENTS.md` |
| `ExtensionBasedTest.java` | Shows JUDO testkit extensions `JudoRuntimeExtension`, `JudoDatasourceByClassExtension`, `JudoDatasourceSingletonExtension` via `@RegisterExtension` and manual `JudoRuntimeFixture` wiring. → see `ExtensionBasedTest.java.AGENTS.md` |
| `InterceptorIntegrationTest.java` | Tests custom dispatcher interceptors with `SpyInterceptor`, `JudoRuntimeFixture.addInterceptor(...)`/`getInterceptorProvider()` and `ReferenceInjector`. → see `InterceptorIntegrationTest.java.AGENTS.md` |
| `JudoTestAnnotationExamples.java` | Tour of `@JudoTest` options: transaction modes AUTO_ROLLBACK/AUTO_COMMIT/MANUAL/NONE, `truncateTables`, `modelName`, `dialect`, dual fixture injection. → see `JudoTestAnnotationExamples.java.AGENTS.md` |