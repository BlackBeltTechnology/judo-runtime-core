# JettyContainer.java

| File | Purpose |
| --- | --- |
| `JettyContainer.java` | Owns embedded Jetty lifecycle; `@Builder` ctor runs `start()` which sizes `QueuedThreadPool` from `maxThreads`/`minThreads`/`idleTimeout`, adds `ServerConnector` on `port` (<=0 maps to 8080), mounts `ServletContextHandler` (SESSIONS) with `SessionHandler` at `contextPath`, then `webServer.start()`; `stop()` loops until `isStopped`. Exposes `servletContextHandler` and qualifier-injectable `port`/`contextPath`/thread settings via `JettyConfigurations`. |