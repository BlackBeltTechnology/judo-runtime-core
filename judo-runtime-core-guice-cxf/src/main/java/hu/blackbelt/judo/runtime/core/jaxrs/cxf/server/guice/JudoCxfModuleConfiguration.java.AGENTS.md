# `JudoCxfModuleConfiguration.java`

Lombok `@Builder @Getter @Setter` value holder of every CXF/CORS option, with `DEFAULT` = all-defaults instance.
Defaults: server `http://localhost` path `api`, WADL/metrics/logging/log-exception/runtime-exceptions/business-cause and `exchangeIdInterceptors` true, CORS origin `*` with credentials true, `corsMaxAge` -1, preflight error status 400.
`cxfDefaultRequestContentType` comes from `SetDefaultContentTypePreMatchContainerRequestFilter.APPLICTION_JSON`.