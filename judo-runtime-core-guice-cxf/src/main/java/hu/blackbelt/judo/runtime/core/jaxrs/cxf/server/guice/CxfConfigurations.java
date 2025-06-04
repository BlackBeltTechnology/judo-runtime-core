package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import javax.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class CxfConfigurations {

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfJaxRsServerUrl {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfJaxRsServerPath {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfSkipDefaultJsonProviderRegistration {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfWadlServiceDescriptionAvailable {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfMetricsEnabled {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfLoggingEnabled {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfLogException {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfReturnRuntimeExceptions {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfIncludeBusinessCause {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfDefaultRequestContentType {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfCorsAllowOrigin {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfCorsAllowCredentials {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfCorsAllowHeaders {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfCorsExposeHeaders {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfCorsMaxAge {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfCorsPrefligthErrorStatus {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfCorsBlockIfUnauthorized {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface CxfCorsDefaultOptionsMethodsHandlePreflight {}

}
