# AGENTS.md — `judo-runtime-core-guice-hsqldb/src/test/resources`

Test-time logging configuration shared by the guice-hsqldb integration tests.

| File | Purpose |
| --- | --- |
| `logback.xml` | Logback config for guice-hsqldb tests. Console appender `CONSOLE` emits `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`; root level set to `warn`; `LevelChangePropagator` with `resetJUL` true forwards java.util.logging to logback. |