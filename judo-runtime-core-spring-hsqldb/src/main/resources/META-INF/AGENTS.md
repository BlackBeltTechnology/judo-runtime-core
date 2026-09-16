# AGENTS.md — `judo-runtime-core-spring-hsqldb/src/main/resources/META-INF`

| File | Purpose |
| --- | --- |
| `spring.factories` | Spring Boot 2.x auto-configuration registration: maps `org.springframework.boot.autoconfigure.EnableAutoConfiguration` to `hu.blackbelt.judo.runtime.core.spring.hsqldb.JudoHsqldbSpringConfiguration`. Callers must keep the module on the runtime classpath; apps without spring-boot-autoconfigure ignore the entry. |