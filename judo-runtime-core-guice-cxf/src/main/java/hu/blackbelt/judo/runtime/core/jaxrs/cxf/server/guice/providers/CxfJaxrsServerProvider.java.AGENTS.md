# AGENTS.md — `CxfJaxrsServerProvider.java`

Guice `Provider<CxfJaxrsServerProvider.ServerHolder>` that bootstraps one CXF JAX-RS server per
injected `Application` onto the Jetty `ServletContextHandler`.

Exports:

- nested `ServerHolder` with `Map<Application, Server> getServers()`
- `get()`, which mounts a `CXFServlet` at `cxfJaxRsServerPath` and creates one `Server` per
  `Application` via `RuntimeDelegate.createEndpoint(application, JAXRSServerFactoryBean.class)`
- package-visible `setupCxfBus`, `setupCxf`, `setupCxfInterceptors`, `setupCxfProviders`,
  `setupCxFeatures`
- optional `@Inject` fields `cxfJaxRsServerUrl`, `cxfJaxRsServerPath`,
  `skipDefaultJsonProviderRegistration`, `wadlServiceDescriptionAvailable`, `metricsEnabled`,
  `loggingEnabled`, `applications`, plus `@CxfQualifiers`-tagged sets `inInterceptors`,
  `outInterceptors`, `faultInterceptors`, `providers`

Contracts:

- An `Application` with neither resource classes nor singletons logs
  `No resource classes found, do not start JAX-RS application` and `get()` returns `null`.
- `setupCxfBus` sets the bus properties `skip.default.json.provider.registration` and
  `wadl.service.description.available` only when the corresponding injected booleans are non-null.
- `MetricsFeature`/`LoggingFeature` are added or removed from the factory features per
  `metricsEnabled`/`loggingEnabled` (both default to `true`).
- Interceptor and provider names are only logged, never validated.