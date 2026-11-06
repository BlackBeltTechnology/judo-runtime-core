# `InMemorySequence.java`

Implements `Sequence<Long>` over a `static ConcurrentHashMap<String, AtomicLong>`. Exports `getNextValue(String)` (adds `increment`, else `incrementAndGet`) and `getCurrentValue(String)`.
`start`/`increment` default to `DEFAULT_START`/`DEFAULT_INCREMENT`; a fresh sequence starts at `start - increment`, so first value equals `start`.
Map is `static` and non-persistent — shared JVM-wide, reset on restart.