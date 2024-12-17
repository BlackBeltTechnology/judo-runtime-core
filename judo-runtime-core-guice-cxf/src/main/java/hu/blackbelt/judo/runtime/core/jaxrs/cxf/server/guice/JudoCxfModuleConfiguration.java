package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import hu.blackbelt.judo.runtime.core.jaxrs.providers.SetDefaultContentTypePreMatchContainerRequestFilter;
import hu.blackbelt.judo.runtime.core.utils.RuntimeVariableResolver;
import lombok.*;

@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class JudoCxfModuleConfiguration {
    public static final JudoCxfModuleConfiguration DEFAULT = JudoCxfModuleConfiguration.builder().build();
    @Builder.Default
    RuntimeVariableResolver runtimeVariableResolver = null;
    @Builder.Default
    Boolean exchangeIdInterceptors = true;
    @Builder.Default
    String cxfJaxRsServerUrl = "http://localhost";
    @Builder.Default
    String cxfJaxRsServerPath = "api";
    @Builder.Default
    Boolean cxfSkipDefaultJsonProviderRegistration = false;
    @Builder.Default
    Boolean cxfWadlServiceDescriptionAvailable = true;
    @Builder.Default
    Boolean cxfMetricsEnabled = true;
    @Builder.Default
    Boolean cxfLoggingEnabled = true;
    @Builder.Default
    Boolean cxfLogException = true;
    @Builder.Default
    Boolean cxfReturnRuntimeExceptions = true;
    @Builder.Default
    Boolean cxfIncludeBusinessCause = true;
    @Builder.Default
    String cxfDefaultRequestContentType = SetDefaultContentTypePreMatchContainerRequestFilter.APPLICTION_JSON;
    @Builder.Default
    String corsAllowOrigin = "*";
    @Builder.Default
    Boolean corsAllowCredentials = true;
    @Builder.Default
    String corsAllowHeaders = "Content-Type,Origin,Accept,Authorization,X-Judo-SignedIdentifier,X-Judo-CountRecords";
    @Builder.Default
    String corsExposeHeaders = "X-Exchange-Id,X-Fault,X-Judo-Count";
    @Builder.Default
    Integer corsMaxAge = -1;
    @Builder.Default
    Integer corsPrefligthErrorStatus = 400;
    @Builder.Default
    Boolean corsBlockIfUnauthorized = false;
    @Builder.Default
    Boolean corsDefaultOptionsMethodsHandlePreflight = false;
}
