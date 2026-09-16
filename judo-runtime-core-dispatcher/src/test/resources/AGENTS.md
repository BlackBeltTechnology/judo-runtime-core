# AGENTS.md — `resources`

| File | Purpose |
| --- | --- |
| `logback.xml` | Routes test logging to stdout. Declares `CONSOLE` `ConsoleAppender` with `PatternLayout` `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`; `root` level `warn`; `LevelChangePropagator` with `resetJUL` true bridging JUL levels. Callers expecting sub-warn log lines on the console must raise root level. |