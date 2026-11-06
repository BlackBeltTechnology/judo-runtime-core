# `QueryModelLoadOrBuild.java`

The one place that decides between restoring a persisted query model and building it; Guice, Spring, OSGi and test fixtures call it. Implements `Supplier<QueryFactory>`; `get()` takes the decision on every call.

**API.** `QueryModelLoadOrBuild.queryModelLoadOrBuild()` builder (the `QueryModelLoadOrBuildBuilder` also has `get()` = `build().get()`):
`asmModel` (required), `asmDigest`, `measureModel` (null = none), `measureDigest` (null = none), `expressionModel`, `expressionDigest`, `persistedQueryModel` (`PersistedQueryModel` locator or null), `customJoinDefinitions` (runtime-core `CustomJoinDefinition` map, null/empty = none), `mode(Mode)` / `mode(String)` (null/blank = `auto`, case-insensitive, unknown → `IllegalArgumentException`), `buildQueryFactory(Supplier<QueryFactory>)` (required: the container's existing build). Digests are SHA-256 hex of the complete model files as loaded (`QueryModelPersistence.DigestInputStream` / `sha256Hex`).

**Decision table (design D5), `auto`; `load` fails with `IllegalStateException("Query model mode is load, but <reason>")` on every build row.**
| Row | auto | log |
| --- | --- | --- |
| mode `build` | build, locator never opened | DEBUG |
| custom joins non-empty | build | INFO "saved query model not used with custom joins" (load: "cannot be used with custom joins") |
| no locator / null stream / no expression model / no ASM or expression digest | build | INFO, reason says "missing" / "not available" |
| any `Exception` or `Error` while reading, parsing, resolving, validating, restoring | build | WARN with cause |
| unresolved proxies > 0 | build | WARN "unresolved proxies: N (samples)" |
| `formatVersion` / `generator` / `modelName` / `asmDigest` / `measureDigest` / `expressionDigest` mismatch (`QueryModelPersistence.checkStamps`) | build | WARN "<stamp> mismatch: artifact A, expected B" (several joined with `; `) |
| otherwise | `QueryFactory.restore` | INFO "Persisted query model restored in N ms" |

Proxies and stamp mismatches are collected together in one WARN. The locator is opened only after the cheap rows (custom joins, missing inputs) and never in `build` mode; the stream is closed by this class.

Tests: `QueryModelLoadOrBuildTest` (every row x mode as dynamic tests, plus TP-29..TP-43 line texts). See change: persisted-query-model
