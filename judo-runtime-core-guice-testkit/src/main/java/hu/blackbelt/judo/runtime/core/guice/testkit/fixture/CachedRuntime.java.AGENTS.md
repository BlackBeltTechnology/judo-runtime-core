# `CachedRuntime.java`

Bundle of one built runtime reused across test methods: `modelLoader`, `dialect`, `queryFactory`, `coercer`, `databaseModule`, `liquibaseExecutor`, `injector`, `transactionManager`, `interceptorProvider`.
Implements `ExtensionContext.Store.CloseableResource`; `close()` guards on an `AtomicBoolean` so a second close is a no-op.
`interceptorProvider` must be carried — cached fixtures skip `init(...)`.