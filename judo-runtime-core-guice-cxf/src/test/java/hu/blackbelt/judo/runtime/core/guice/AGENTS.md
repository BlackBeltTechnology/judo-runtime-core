# AGENTS.md — guice

| File | Purpose |
| --- | --- |
| `CalcApplication.java` | JAX-RS `Application` under `@ApplicationPath("Calc")` publishing the calc sample. Exports `setCalcService(CalcService)`, `getSingletons()`. Returns `Set.of(calcService)`. Callers must inject or set `calcService` before `getSingletons()`; unset field yields set holding null. |
| `CalcModule.java` | Guice `AbstractModule` for the calc test sample. `configure()` binds `CalcService.class` to a new instance and registers `CalcApplication` via `Multibinder<Application>` set binding. |
| `CalcService.java` | JAX-RS resource at `@Path("/calc")` for the Guice integration test. Exports `add(double, double)` on `@GET @Path("/add/{a}/{b}")`, `@Produces(MediaType.APPLICATION_JSON)`. Holds `@Inject DAO dao`; injector must satisfy it. |
| `JudoCxfModuleTest.java` | Integration test booting Guice + HSQLDB + Jetty + CXF with the calc sample. → see `JudoCxfModuleTest.java.AGENTS.md` |