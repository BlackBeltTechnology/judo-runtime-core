# `CxfConfigurations.java`

Namespace of 18 `@Qualifier @BindingAnnotation` nested annotations naming each CXF option binding: `CxfJaxRsServerUrl`, `CxfJaxRsServerPath`, `CxfSkipDefaultJsonProviderRegistration`, `CxfWadlServiceDescriptionAvailable`, `CxfMetricsEnabled`, `CxfLoggingEnabled`, `CxfLogException`, `CxfReturnRuntimeExceptions`, `CxfIncludeBusinessCause`, `CxfDefaultRequestContentType`, and eight `CxfCors*` keys.
Declares no fields — injection sites bind `String`/`Boolean`/`Integer` by annotation, so type must match what `configureOptions` binds.