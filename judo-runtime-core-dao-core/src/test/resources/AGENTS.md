# AGENTS.md — `judo-runtime-core-dao-core/src/test/resources`

Test logging configuration shared by dao-core unit tests.

| File | Purpose |
| --- | --- |
| `logback.xml` | Logback config for dao-core tests: single `CONSOLE` appender with `PatternLayout` `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`, root level `warn`, `LevelChangePropagator` context listener with `resetJUL=true` syncing Jul levels. |