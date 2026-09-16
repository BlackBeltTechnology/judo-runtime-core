# AGENTS.md — `judo-runtime-core-spring-postgresql/src/main/resources/META-INF/spring`

Spring Boot ≥ 2.7 auto-configuration registration for the PostgreSQL binding of
the runtime: which configuration class the context activates on startup.

| File | Purpose |
| --- | --- |
| `org.springframework.boot.autoconfigure.AutoConfiguration.imports` | Registers `hu.blackbelt.judo.runtime.core.spring.postgresql.JudoPostgresqlSpringConfiguration` for `EnableAutoConfiguration` via the Boot ≥ 2.7 imports file, replacing `spring.factories`. FQCN must stay on classpath; class gated by `@ConditionalOnExpression("'${spring.datasource.url}'.contains('postgresql')")`, so a non-postgresql or absent URL skips the bean set. |