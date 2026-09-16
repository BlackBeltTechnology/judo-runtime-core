# AGENTS.md — `DefaultVariableResolver.java`

Implements `VariableResolver` (read side) and `VariableResolverManager` (registration side)
over two `ConcurrentHashMap` registries plus a `HashSet` of cacheable keys.

Exports:

- `<T> T resolve(Class<T> type, String category, String key)`
- `void registerSupplier(String category, String key, Supplier supplier, boolean cacheable)`
- `void registerFunction(String category, Function function, boolean cacheable)`
- `void unregisterSupplier(String category, String key)`
- `void unregisterFunction(String category)`

Contracts:

- Registry key is `category + ":" + key`; function registrations are keyed by `category` alone
  and cache-flagged as `category + ":*"`.
- `resolve` checks suppliers first, then the per-category function; a supplier registered for the
  exact key shadows the category function.
- Resolved values are memoised per request in `Context` under `Dispatcher.VARIABLES_KEY` only when
  the key was registered `cacheable`; a non-cacheable variable is recomputed on every `resolve`.
- The returned value is always passed through `dataTypeManager.getCoercer().coerce(value, type)`,
  so providers may return loosely typed values.
- An unregistered `category`/`key` is not an error: it logs `Undefined variable: {}.{}` and
  resolves to `null` (coerced).
- `dataTypeManager` and `context` are `@NonNull` and must be set before the first `resolve`.
