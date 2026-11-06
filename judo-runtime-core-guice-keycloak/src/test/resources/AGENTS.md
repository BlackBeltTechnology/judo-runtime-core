# AGENTS.md — `judo-runtime-core-guice-keycloak/src/test/resources`

Test logging configuration for guice-keycloak tests.

| File | Purpose |
| --- | --- |
| `logback.xml` | Logback config for keycloak Guice tests: single `CONSOLE` appender with `PatternLayout` `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`, root level `info`, `LevelChangePropagator` context listener with `resetJUL=true` syncing JUL levels. |