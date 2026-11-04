# AGENTS.md — `judo-runtime-core-guice/src/main/java/hu/blackbelt/judo/runtime/core/guice`

Guice DI wiring for the JUDO runtime: the default module, its configuration holder, model loading, component scanning, and binding qualifiers.

| File | Purpose |
| --- | --- |
| `ComponentScanModule.java` | `ComponentScanModule extends AbstractModule` scanning `packageName` via `org.reflections.Reflections` and binding types annotated with the passed `bindingAnnotations`. → see `ComponentScanModule.java.AGENTS.md` |
| `JudoConfigurationQualifiers.java` | Nest of empty Guice binding qualifiers, each `@Qualifier`+`@BindingAnnotation`, FIELD/PARAMETER/METHOD targets, `RUNTIME` retention — `QueryFactoryCustomJoinDefinitions` through `RdbmsSequenceCreateIfNotExists`. → see `JudoConfigurationQualifiers.java.AGENTS.md` |
| `JudoDefaultModule.java` | Main runtime DI module — chains ~26 per-service configurers, model and option bindings, secret generation. → see `JudoDefaultModule.java.AGENTS.md` |
| `JudoDefaultModuleConfiguration.java` | Lombok `@Builder @Getter @Setter @AllArgsConstructor @NoArgsConstructor` config holder, static `DEFAULT`, ~48 fields from `injectModulesTo` to `platformTransactionManager`. → see `JudoDefaultModuleConfiguration.java.AGENTS.md` |
| `JudoModelLoader.java` | Loads the full metamodel set (ASM/RDBMS/Measure/Expression/Liquibase/Keycloak + traces) for one model name + dialect from classpath/directory/URL, plus `empty()`. → see `JudoModelLoader.java.AGENTS.md` |