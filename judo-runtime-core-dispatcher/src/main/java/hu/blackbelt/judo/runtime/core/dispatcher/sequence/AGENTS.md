# AGENTS.md — `judo-runtime-core-dispatcher/src/main/java/hu/blackbelt/judo/runtime/core/dispatcher/sequence`

Non-persistent `Sequence` implementation used when no database sequence provider is wired in.

| File | Purpose |
| --- | --- |
| `InMemorySequence.java` | Non-persistent in-JVM `Sequence<Long>` over `static ConcurrentHashMap<String, AtomicLong>`. Exports `getNextValue(String)`, `getCurrentValue(String)`. → see `InMemorySequence.java.AGENTS.md` |
