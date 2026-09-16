# AGENTS.md — `judo-runtime-core-guice-postgresql/src/test/resources`

Test logging configuration for the PostgreSQL guice module tests.

| File | Purpose |
| --- | --- |
| `logback.xml` | Logback config for PostgreSQL module tests: `CONSOLE` `ConsoleAppender` with `PatternLayout` `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`, root level `INFO`, `LevelChangePropagator` with `resetJUL=true` so JUL logging follows logback. |