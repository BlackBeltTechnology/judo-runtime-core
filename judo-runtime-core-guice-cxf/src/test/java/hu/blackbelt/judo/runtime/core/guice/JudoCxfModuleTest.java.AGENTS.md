# AGENTS.md — JudoCxfModuleTest.java

| Aspect | Detail |
| --- | --- |
| Purpose | Integration test booting the full Guice runtime stack — `JudoDefaultModule` + `JudoHsqldbModule` + `JudoJettyModule` + `JudoCxfModule` + `CalcModule` — and calling the published `Calc` endpoint over HTTP. |
| Key exports | `testAddCall()` JUnit test. State: `injector` (`com.google.inject.Injector`), `port`, `restUri`, `@Inject CxfJaxrsServerProvider.ServerHolder serverHolder`. |
| `@BeforeEach init()` | Allocates free TCP port via `new ServerSocket(0).getLocalPort()`, builds `Guice.createInjector(Modules.combine(judoModule, sqlModule, jettyModule, cxfModule, calcModule))`, calls `injectorMembers(cxfModule)` and `injectorMembers(this)`, sets `restUri = "http://localhost:" + port + "/api/Calc"`. |
| `@AfterEach destroy()` | Destroys every server in `CxfJaxrsServerProvider.ServerHolder.getServers().values()` and stops `JettyContainer`. |
| `testAddCall()` | CXF `WebClient.create(restUri)` → `.path("calc/add").path("122/34").accept("application/json")` → `get(String.class)` asserts equals `"156.0"`. |
| Contracts / invariants | Requires free TCP port and working HSQLDB + Jetty + CXF modules; `serverHolder` must be field-injected before `destroy()`; `JudoHsqldbModule` needs H2/HSQLDB on classpath; asserting `"156.0"` pins JAX-RS JSON serialization of `Double`. |