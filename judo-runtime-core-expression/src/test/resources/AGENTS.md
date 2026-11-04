# AGENTS.md — resources

| File | Purpose |
| --- | --- |
| `logback.xml` | Configures test logging. Defines `CONSOLE` `ConsoleAppender` with `PatternLayout` pattern `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`; sets `root` level `warn`; installs `LevelChangePropagator` `contextListener` with `resetJUL`. |