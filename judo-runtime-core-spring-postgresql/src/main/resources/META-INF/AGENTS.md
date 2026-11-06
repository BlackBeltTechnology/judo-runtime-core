# AGENTS.md — `judo-runtime-core-spring-postgresql/src/main/resources/META-INF`

| File | Purpose |
| --- | --- |
| `spring.factories` | Spring Boot `EnableAutoConfiguration` registration: maps key `org.springframework.boot.autoconfigure.EnableAutoConfiguration` to `hu.blackbelt.judo.runtime.core.spring.postgresql.JudoPostgresqlSpringConfiguration`. Removing the entry stops PostgreSQL auto-config from loading. |