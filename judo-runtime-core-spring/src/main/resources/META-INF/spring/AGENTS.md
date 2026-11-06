# AGENTS.md — `judo-runtime-core-spring/src/main/resources/META-INF/spring`

| File | Purpose |
| --- | --- |
| `org.springframework.boot.autoconfigure.AutoConfiguration.imports` | Registers Spring Boot auto-configuration classes loaded at startup: `JudoModelLoaderConfiguration`, `JudoModelConfiguration`, `JudoDefaultSpringConfiguration`, `JudoBaseServiceConfiguration`, `AntlrCheckConfiguration`. File order = registration order; removing an entry disables that config's auto-load. |