# AGENTS.md — `judo-runtime-core-guice-testkit/src/test/resources`

| File | Purpose |
| --- | --- |
| `logback.xml` | Logback config for guice-testkit tests: `CONSOLE` appender with `PatternLayout` `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`, root level `warn`, `LevelChangePropagator` context listener `resetJUL=true` syncing JUL levels. |