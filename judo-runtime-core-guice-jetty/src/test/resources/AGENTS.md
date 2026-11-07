# AGENTS.md — `judo-runtime-core-guice-jetty/src/test/resources`

Test-time logging configuration for the jetty Guice integration tests.

| File | Purpose |
| --- | --- |
| `logback.xml` | Logback config for guice-jetty tests. Console appender `CONSOLE` emits `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`; root level set to `info`; `LevelChangePropagator` with `resetJUL` true forwards java.util.logging to logback. |