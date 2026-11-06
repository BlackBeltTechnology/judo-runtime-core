# AGENTS.md — `judo-runtime-core-spring/src/test/java/hu/blackbelt/judo/runtime/core/spring`

JUnit 5 tests of the Spring configuration and model loader over the generated Northwind model set. See change: persisted-query-model

| File | Purpose |
| --- | --- |
| `JudoModelLoaderQueryModelTest.java` | Loader digests and locator for directory / classpath jar / file URL x present / absent / corrupt / stale (as the Guice loader test). |
| `LogCapture.java` | Test helper. Captures the events (INFO and above) of one class' logger (logback); `only(Level)`, `warnings()`; restores the level on `close()`. |
| `ModelSources.java` | Test helper. Writes the Northwind file set into a directory, jar or behind a file URL with the query model in a state (`PRESENT`, `ABSENT`, `CORRUPT`, `STALE`); `write(...)` also serves the classpath tests; loads it with the Spring `JudoModelLoader`. |
| `NorthwindModelSet.java` | Test helper. Generates the full Northwind model file set once per JVM plus the saved query model stamped with the digests of those bytes. |
| `QueryModelSpringContextTest.java` | TP-24 `AnnotationConfigApplicationContext` over the production `JudoModelLoaderConfiguration`, `JudoModelConfiguration`, `JudoDefaultSpringConfiguration` with the files in `target/test-classes/model`: `QueryFactory` bean restored (INFO), exactly one `ExpressionModel` bean (the loader's), one `PersistedQueryModel` bean; none without the artifact; state x `judo.queryModel.mode` matrix (`load` fails the start, `build` never opens the artifact); application with its own model beans builds. Collaborators are mocks, all bean methods but `getQueryFactory` are lazy. |
