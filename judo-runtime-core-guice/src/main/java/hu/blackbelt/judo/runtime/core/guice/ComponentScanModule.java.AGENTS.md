# AGENTS.md — `ComponentScanModule.java`

| File | Purpose |
| --- | --- |
| `ComponentScanModule.java` | `ComponentScanModule extends AbstractModule` binding every class annotated with the passed `bindingAnnotations` found in `packageName`. Constructor `@SafeVarargs ComponentScanModule(String packageName, Class<? extends Annotation>... bindingAnnotations)`; `configure()` runs `org.reflections.Reflections.getTypesAnnotatedWith(annotation)` per annotation and `bind(Class)` each hit. Contract: only types tagged with one of `bindingAnnotations` inside `packageName` get bound. |