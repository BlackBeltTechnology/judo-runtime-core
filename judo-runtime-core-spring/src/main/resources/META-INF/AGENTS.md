# AGENTS.md — `judo-runtime-core-spring/src/main/resources/META-INF`

Legacy Boot auto-configuration registration: the `@Configuration` class names
Spring instantiates on context startup via `spring.factories`.

| File | Purpose |
| --- | --- |
| `spring.factories` | `EnableAutoConfiguration` registrar (line-continuation format) listing 5 `@Configuration` classes: `JudoModelLoaderConfiguration`, `JudoModelConfiguration`, `JudoDefaultSpringConfiguration`, `JudoBaseServiceConfiguration`, `AntlrCheckConfiguration`. Boot activates every listed class; a wrong FQCN or missing class on the classpath fails context refresh. |