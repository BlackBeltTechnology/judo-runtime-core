# AGENTS.md — `judo-runtime-core-guice-cxf/src/main/java/hu/blackbelt/judo/runtime/core/jaxrs/cxf/server/guice`

Guice wiring of the CXF JAX-RS server: one `AbstractModule`, its value holder, and the two
binding-annotation namespaces (per-option config keys, per-collection multibinder keys)
that the `providers` subpackage injects against.

| File | Purpose |
| --- | --- |
| `CxfConfigurations.java` | Namespace of 18 `@Qualifier @BindingAnnotation` CXF option-binding annotations (`CxfJaxRsServerUrl`…`CxfCors*`), field-less. → see `CxfConfigurations.java.AGENTS.md` |
| `CxfQualifiers.java` | Namespace of the four multibinder keys: `Providers` (set of `Object`), `InInterceptors`, `OutInterceptors`, `FaultInterceptors` (sets of `Interceptor`). All `@Qualifier @BindingAnnotation`, runtime-retained, targeting field/parameter/method. Contributors must use the same annotation `JudoCxfModule` passed to `Multibinder.newSetBinder`, or their binding lands in a different set. |
| `JudoCxfModule.java` | → see `JudoCxfModule.java.AGENTS.md` — Guice module publishing the CXF JAX-RS server plus its provider and interceptor sets. |
| `JudoCxfModuleConfiguration.java` | Lombok `@Builder @Getter @Setter` value holder of every CXF/CORS option with `DEFAULT` all-defaults instance. → see `JudoCxfModuleConfiguration.java.AGENTS.md` |
