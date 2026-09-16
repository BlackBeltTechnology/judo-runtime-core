# AGENTS.md — `judo-runtime-core-guice-hsqldb/src/test/java/hu/blackbelt/judo/runtime/core/guice/hsqldb`

Package-level JUnit tests wiring the HSQLDB-backed guice module.

| File | Purpose |
| --- | --- |
| `JudoDefaultHsqldbModuleTest.java` | JUnit 5 smoke test that `JudoDefaultModule` combined with `JudoHsqldbModule.builder().build()` boots a Guice `Injector` over `JudoModelLoader.empty()`; `@BeforeEach init()` times injector creation with `Stopwatch`, `test()` asserts `injector.getInstance(DAO.class)` non-null. Fails when the hsqldb module cannot bind `DAO`. |