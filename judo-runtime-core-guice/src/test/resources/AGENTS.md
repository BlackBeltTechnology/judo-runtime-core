# AGENTS.md — `judo-runtime-core-guice/src/test/resources`

Test logging configuration for the base guice module tests.

| File | Purpose |
| --- | --- |
| `logback.xml` | Logback config for base guice module tests: `CONSOLE` `ConsoleAppender` with `PatternLayout` `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`, root level `warn`, `LevelChangePropagator` with `resetJUL=true` so JUL logging follows logback. |