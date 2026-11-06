# AGENTS.md — `JudoModelLoader.java`

| File | Purpose |
| --- | --- |
| `JudoModelLoader.java` | Loads the full metamodel set for one JUDO model: `AsmModel`, `RdbmsModel`, `MeasureModel`, `ExpressionModel`, `LiquibaseModel`, optional `KeycloakModel`, plus `Asm2RdbmsTransformationTrace`/`Asm2KeycloakTransformationTrace`. Exports Lombok `@Builder` ctor, getters for all models and `useKeycloak`, and static loaders: `loadFromClassloader(modelName, ClassLoader, Dialect, validate, loadKeycloak)` (enumerates `model` classpath resources, resolves `modelName-asm.model`), `loadFromDirectory(modelName, File, Dialect[, validate], loadKeycloak)`, `loadFromURL(modelName, URI, Dialect, validate, loadKeycloak)`, `load(modelName, File, Dialect)` (builder), `empty()` (builds blank `judo` ASM EPackage, registers RDBMS mapping metamodels, injects hsqldb excel mappings into `RdbmsModel`, blank Liquibase/Keycloak models with empty traces). Contract: expects files named `<modelName>-asm.model`, `-rdbms_<dialect>.model`, `-measure.model`, `-expression.model`, `-liquibase_<dialect>.changelog.xml` (and `-asm2rdbms_<dialect>.model`, `-asm2keycloak.model` when `loadKeycloak`) under the given URI; throws `IllegalArgumentException` when model name mismatches the ASM name, directory is null/missing/not a directory, or args are null. |


## Persisted query model (See change: persisted-query-model)

- Fields `asmDigest`, `measureDigest`, `expressionDigest` (SHA-256 hex) and `persistedQueryModel` (`PersistedQueryModel`); `null` for loaders built by hand or by `empty()`.
- `loadFromURL` loads ASM, measure and expression model through private `DigestedStream`: a `QueryModelPersistence.DigestInputStream` behind a stream that ignores `close()`. `digest()` drains the file to EOF after the model load, so parser read-ahead cannot make the digest cover less than the complete file. The stream is closed afterwards.
- Locator: `<modelName>-query.model` next to the other files. `exists(URI)` never reads it: `file:` -> `File.isFile()`, `jar:` -> `JarURLConnection.getJarEntry()`, other protocols -> open and close. Present -> `new PersistedQueryModel(modelName, () -> uri.toURL().openStream())`; absent -> `null`. A corrupt file is still located (the decision reads it).
- Works the same for `loadFromDirectory`, `loadFromClassloader` and `loadFromURL` (they all end in `loadFromURL`).
- Tests: `src/test/java/.../guice/JudoModelLoaderQueryModelTest.java` (dir / classpath jar / URL x present / absent / corrupt / stale).
