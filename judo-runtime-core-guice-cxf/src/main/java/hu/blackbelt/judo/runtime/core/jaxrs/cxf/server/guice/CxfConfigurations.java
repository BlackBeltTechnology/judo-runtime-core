package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import com.google.inject.Inject;

import javax.annotation.Nullable;
import javax.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public class CxfConfigurations {

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfJaxRsServerPort {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfJaxRsServerUrl {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfJaxRsServerPath {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfSkipDefaultJsonProviderRegistration {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfWadlServiceDescriptionAvailable {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfMetricsEnabled {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfLoggingEnabled {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfLogException {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfReturnRuntimeExceptions {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfIncludeBusinessCause {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfDefaultRequestContentType {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfCorsAllowOrigin {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfCorsAllowCredentials {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfCorsAllowHeaders {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfCorsExposeHeaders {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfCorsMaxAge {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfCorsPrefligthErrorStatus {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfCorsBlockIfUnauthorized {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CxfCorsDefaultOptionsMethodsHandlePreflight {}

}
