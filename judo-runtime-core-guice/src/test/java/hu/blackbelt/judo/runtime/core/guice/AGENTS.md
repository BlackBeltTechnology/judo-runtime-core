# AGENTS.md — `judo-runtime-core-guice/src/test/java/hu/blackbelt/judo/runtime/core/guice`

JUnit 5 tests of the Guice entry point and model loader over the generated Northwind model set. See change: persisted-query-model

| File | Purpose |
| --- | --- |
| `JudoDefaultModuleQueryModelTest.java` | TP-25 the injector has exactly one `ExpressionModel` binding (the loader's instance) and no query-side or `PersistedQueryModel` binding; `queryModelMode` default `auto`, copied by the builder, configuration object wins, reaches the decision (strict `load` without artifact fails injector creation); the `QueryFactory` is restored eagerly at injector creation; a supplied `QueryFactory` bypasses the decision. `Stage.DEVELOPMENT` over `TestBindings`. |
| `JudoModelLoaderQueryModelTest.java` | Loader digests (SHA-256 of the file bytes read) and locator for directory / classpath jar / file URL x present / absent / corrupt / stale; the locator streams the query file; a corrupt file is located but never read by the loader. |
| `LogCapture.java` | Test helper. Captures the events (INFO and above) of one class' logger (logback); `only(Level)`, `warnings()`; restores the level on `close()`. |
| `ModelSources.java` | Test helper. Writes the Northwind file set into a directory, a jar under `model/` (own `URLClassLoader`) or behind a file URL, with the query model `PRESENT` / `ABSENT` / `CORRUPT` (truncated at 50%) / `STALE` (expression file one byte longer), and loads it with `JudoModelLoader`; `cleanUp()`. |
| `NorthwindModelSet.java` | Test helper. Generates the full Northwind model file set once per JVM (psm2asm, psm2measure, asm2expression, asm2rdbms, rdbms2liquibase for `hsqldb`) plus the saved query model stamped with the digests of those bytes. |
| `QueryFactoryProviderTest.java` | TP-23 `QueryFactoryProvider` restores from dir / classpath jar / URL in mode `auto` (INFO "query model restored ... ms", no WARN, JQL extractor spy count 0); matrix source x state x mode (`auto`, `load`, `build`, default): fallback WARNs, strict `load` failures, `build` never restores. The JQL extractor is replaced by a construction mock that counts and throws. |
| `TestBindings.java` | Test helper. Stub bindings (`Dialect`, `DataSource`, `RdbmsInit`, `RdbmsParameterMapper`, `MapperFactory`, `Sequence`) a database module would provide, so `JudoDefaultModule` creates an injector without a database. |
