# AGENTS.md — `ThreadContext.java`

Implements `hu.blackbelt.judo.dispatcher.api.Context` over a static `ThreadLocal<Map<String, Object>>`;
`putIfAbsent`, `remove` and `removeAll` synchronise on the per-thread map.

Exports:

- `get(String key)`, `<T> T getAs(Class<T> clazz, String key)`, `put`, `putIfAbsent`, `remove`,
  `removeAll`
- static constants `DEBUG_THREAD_FORK = "debugThreadFork"`, `INHERITABLE_CONTEXT = "inheritableContext"`
- builder flags `debugThreadFork` (default `false`), `inheritableContext` (default `true`), and a
  `@NonNull dataTypeManager` field

Contracts:

- Every constructor calls `setupThreadLocal()`: with `inheritableContext=true` the static
  `THREADLOCAL` is re-created as an `InheritableThreadLocal` whose `childValue` logs a debug fork
  trace when `debugThreadFork` is set; with `inheritableContext=false` it becomes a plain
  `ThreadLocal`. Rebuilding drops all previously stored values.
- `getAs` coerces the stored value via `dataTypeManager.getCoercer().coerce(value, clazz)`;
  `dataTypeManager` must be set before any `getAs` call.
- `put` throws `IllegalArgumentException` (`checkArgument`) on a null key; a null value removes
  the key instead of storing `null`.
- `putIfAbsent` returns `get(key)` after the operation — not the value it just inserted — so
  callers cannot rely on standard `Map` semantics.
- Per-thread maps are created lazily as `TreeMap` and synchronised on themselves; `removeAll`
  clears the map and drops the thread-local.