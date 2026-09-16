# AGENTS.md — `judo-runtime-core-guice-jetty/src/test/java/hu/blackbelt/judo/runtime/core/jetty/guice`

Integration test for the Jetty Guice bootstrap wiring.

| File | Purpose |
| --- | --- |
| `JudoJettyModuleTest.java` | Boots `JudoJettyModule` through `Guice.createInjector` on a free port from `ServerSocket(0)` with context path `/`, then asserts `JettyContainer.getContextPath()` equals `/`; `@AfterEach` calls `JettyContainer.stop()`. |