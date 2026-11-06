# AGENTS.md — `judo-runtime-core-query/src/test/java/hu/blackbelt/judo/runtime/core/query`

JUnit 5 tests of the runtime query entry point over the Northwind models (judo-tatami-base psm2asm / psm2measure / asm2expression). See change: persisted-query-model

| File | Purpose |
| --- | --- |
| `LogCapture.java` | Test helper. Captures the events (INFO and above) of one class' logger; `warnings()`, `atLeast(Level)`, `only(Level)`; restores the logger level on `close()`. |
| `NorthwindFixture.java` | Test helper. Builds Northwind PSM → ASM, measure and expression models once per call (`create()`) or per JVM (`shared()`); shared instances are read-only. |
| `QueryFactoryBuildTest.java` | Build path: the deprecated `coercer` is never called; custom joins are copied field for field, applied at build time only, and later map additions change nothing. |
| `QueryFactoryRestoreTest.java` | TP-19 restored `QueryFactory` equals built (every public method, every mapped type and feature, mutable empty custom joins); TP-20 restored references are the live ASM / expression objects; restored factory converts dynamic expressions. |
| `QueryModelLoadOrBuildTest.java` | D5 decision table: valid restore, TP-29..TP-35 and TP-38..TP-41 fallbacks with their WARN / INFO text, TP-42 build mode never opens the artifact, TP-43 strict load failures, every row x `auto`/`load`/`build` as `@TestFactory`. |
| `SavedQueryModel.java` | Test helper. A built `QueryFactory` plus its saved artifact, file bytes and SHA-256 digests; `locator(...)` (counts stream openings), `withInfoAttribute(...)` (stale stamps), `decision(...)` (a `QueryModelLoadOrBuild` builder with matching inputs), `load()`. |
