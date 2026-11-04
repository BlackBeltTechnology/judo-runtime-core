# AGENTS.md — `judo-runtime-core-guice-jetty/src/main/java/hu/blackbelt/judo/runtime/core/jetty/guice`

Guice wiring for the embedded Jetty HTTP server: module, container lifecycle, and server-config qualifiers.

| File | Purpose |
| --- | --- |
| `JettyConfigurations.java` | Grouping class owning Guice `@BindingAnnotation` qualifiers `JettyServerPort`, `JettyServerContextPath`, `JettyServerMaxThreads`, `JettyServerMinThreads`, `JettyServerIdleTimeout`; each `@Qualifier` with RUNTIME retention bounded to FIELD/PARAMETER/METHOD targets. |
| `JettyContainer.java` | Owns embedded Jetty lifecycle: builds `Server` with `QueuedThreadPool`/`ServerConnector`/`ServletContextHandler`, starts and stops it. → see `JettyContainer.java.AGENTS.md` |
| `JettyContainerProvider.java` | Guice `Provider<JettyContainer>`; `get()` builds a `JettyContainer` via `JettyContainer.builder()` from optional injected `JettyServerPort` and `JettyServerContextPath`, leaving thread settings to container defaults. |
| `JudoJettyModule.java` | Guice `AbstractModule` enabling the Jetty container; `configure()` binds `JettyContainer` to `JettyContainerProvider` as eager singleton, and `configureOptions()` binds `JettyConfigurations` qualifiers to `JudoJettyModuleConfiguration` values (defaults `jettyServerPort=8181`, `jettyContextPath="/"`, `maxThreads=100`, `minThreads=10`, `idleTimeout=120`). |