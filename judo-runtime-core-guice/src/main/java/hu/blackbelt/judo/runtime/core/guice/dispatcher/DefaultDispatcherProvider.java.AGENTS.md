# `DefaultDispatcherProvider.java`

Guice `Provider<Dispatcher>`. `get()` assembles `DefaultDispatcher` across ~20 injected
collaborators: `DAO`, `AccessManager`, `ActorResolver`, `IdentifierSigner`,
`PayloadValidator`, `ValidatorProvider`, `Export`, plus optional
`PlatformTransactionManager`, `OpenIdConfigurationProvider`, filestore
`TokenIssuer`/`TokenValidator`.

Optional qualifiers `@DispatcherMetricsReturned`, `@DispatcherEnableDefaultValidation`,
`@DispatcherTrimString`, `@DispatcherCaseInsensitiveLike` all pass through to the builder.