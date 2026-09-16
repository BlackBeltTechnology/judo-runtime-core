# `DefaultVariableResolverProvider.java`

Guice `Provider<VariableResolver>`. `get()` creates `DefaultVariableResolver` and
registers suppliers/functions: `SYSTEM` suppliers
`current_timestamp`/`current_date`/`current_time`, plus `ENVIRONMENT`
(`EnvironmentVariableProvider`), `SEQUENCE` (`SequenceProvider` over injected
`Sequence`), `REQUEST` (`RequestParametersVariableProvider` over `Context`).
Function registration order fixed; names must not collide across namespaces.