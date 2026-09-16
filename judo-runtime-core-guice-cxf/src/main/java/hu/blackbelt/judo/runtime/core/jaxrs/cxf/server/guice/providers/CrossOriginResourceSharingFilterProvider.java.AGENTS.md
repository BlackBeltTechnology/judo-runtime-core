# AGENTS.md — `CrossOriginResourceSharingFilterProvider.java`

Guice `Provider<CrossOriginResourceSharingFilter>` mapping CORS settings onto the Apache CXF CORS
filter.

Exports:

- `<CrossOriginResourceSharingFilter> get()`
- eight optional `@Inject @CxfConfigurations.CxfCors*` fields with defaults: `corsAllowOrigin`
  (`*`), `corsAllowCredentials` (`true`), `corsAllowHeaders`
  (`Content-Type,Origin,Accept,Authorization,X-Judo-SignedIdentifier,X-Judo-CountRecords`),
  `corsExposeHeaders` (`X-Exchange-Id,X-Fault,X-Judo-Count`), `corsMaxAge` (`-1`),
  `corsPrefligthErrorStatus` (`400`), `corsBlockIfUnauthorized` (`false`),
  `corsDefaultOptionsMethodsHandlePreflight` (`false`)

Contracts:

- `get()` configures one `CrossOriginResourceSharingFilter` via
  `setAllowOrigins`/`setAllowCredentials`/`setAllowHeaders`/`setExposeHeaders`/`setMaxAge`/
  `setBlockCorsIfUnauthorized`/`setDefaultOptionsMethodsHandlePreflight`/`setPreflightErrorStatus`
  then returns a different, fresh `new CrossOriginResourceSharingFilter()` — the configured
  instance is discarded and the served filter carries only CXF defaults.