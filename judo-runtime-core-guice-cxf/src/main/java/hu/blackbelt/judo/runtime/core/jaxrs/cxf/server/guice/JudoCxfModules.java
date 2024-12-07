package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */


import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers.*;
import hu.blackbelt.judo.runtime.core.jaxrs.providers.SetDefaultContentTypePreMatchContainerRequestFilter;
import lombok.Builder;
import org.apache.cxf.interceptor.Interceptor;

import javax.annotation.Nullable;

public class JudoCxfModules extends AbstractModule {

    private Boolean exchangeIdInterceptors;
    private Integer cxfJaxRsServerPort;
    private String cxfJaxRsServerUrl;
    private String cxfJaxRsServerPath;

    private Boolean cxfSkipDefaultJsonProviderRegistration;
    private Boolean cxfWadlServiceDescriptionAvailable;
    private Boolean cxfMetricsEnabled;
    private Boolean cxfLoggingEnabled;
    private Boolean cxfLogException;

    private Boolean cxfReturnRuntimeExceptions;
    private Boolean cxfIncludeBusinessCause;
    private String cxfDefaultRequestContentType;
    private String corsAllowOrigin;
    private Boolean corsAllowCredentials;
    private String corsAllowHeaders;
    private String corsExposeHeaders;
    private Integer corsMaxAge;
    private Integer corsPrefligthErrorStatus;
    private Boolean corsBlockIfUnauthorized;
    private Boolean corsDefaultOptionsMethodsHandlePreflight;


    public static class JudoCxfModulesBuilder {
        Boolean exchangeIdInterceptors = true;
        Integer cxfJaxRsServerPort = 8181;
        String cxfJaxRsServerUrl = "http://localhost";
        String cxfJaxRsServerPath = "api";
        Boolean cxfSkipDefaultJsonProviderRegistration = false;
        Boolean cxfWadlServiceDescriptionAvailable = true;
        Boolean cxfMetricsEnabled = true;
        Boolean cxfLoggingEnabled = true;
        Boolean cxfLogException = true;
        Boolean cxfReturnRuntimeExceptions = true;
        Boolean cxfIncludeBusinessCause = true;
        String cxfDefaultRequestContentType = SetDefaultContentTypePreMatchContainerRequestFilter.APPLICTION_JSON;
        String corsAllowOrigin = "*";
        Boolean corsAllowCredentials = true;
        String corsAllowHeaders = "Content-Type,Origin,Accept,Authorization,X-Judo-SignedIdentifier,X-Judo-CountRecords";
        String corsExposeHeaders ="X-Exchange-Id,X-Fault,X-Judo-Count";
        Integer corsMaxAge = -1;
        Integer corsPrefligthErrorStatus = 400;
        Boolean corsBlockIfUnauthorized = false;
        Boolean corsDefaultOptionsMethodsHandlePreflight = false;
    }



    @Builder
    private JudoCxfModules(Integer cxfJaxRsServerPort,
                            String cxfJaxRsServerUrl,
                            String cxfJaxRsServerPath,
                            Boolean exchangeIdInterceptors,
                            Boolean cxfSkipDefaultJsonProviderRegistration,
                            Boolean cxfWadlServiceDescriptionAvailable,
                            Boolean cxfMetricsEnabled,
                            Boolean cxfLoggingEnabled,
                            Boolean cxfLogException,
                            Boolean cxfReturnRuntimeExceptions,
                            Boolean cxfIncludeBusinessCause,
                            String cxfDefaultRequestContentType,
                            String corsAllowOrigin,
                            Boolean corsAllowCredentials,
                            String corsAllowHeaders,
                            String corsExposeHeaders,
                            Integer corsMaxAge,
                            Integer corsPrefligthErrorStatus,
                            Boolean corsBlockIfUnauthorized,
                            Boolean corsDefaultOptionsMethodsHandlePreflight
    ) {
        this.cxfJaxRsServerPort = cxfJaxRsServerPort;
        this.cxfJaxRsServerUrl = cxfJaxRsServerUrl;
        this.cxfJaxRsServerPath = cxfJaxRsServerPath;
        this.exchangeIdInterceptors = exchangeIdInterceptors;
        this.cxfSkipDefaultJsonProviderRegistration = cxfSkipDefaultJsonProviderRegistration;
        this.cxfWadlServiceDescriptionAvailable = cxfWadlServiceDescriptionAvailable;
        this.cxfMetricsEnabled = cxfMetricsEnabled;
        this.cxfLoggingEnabled = cxfLoggingEnabled;
        this.cxfLogException = cxfLogException;
        this.cxfReturnRuntimeExceptions = cxfReturnRuntimeExceptions;
        this.cxfIncludeBusinessCause = cxfIncludeBusinessCause;
        this.cxfDefaultRequestContentType = cxfDefaultRequestContentType;
        this.corsAllowOrigin = corsAllowOrigin;
        this.corsAllowCredentials = corsAllowCredentials;
        this.corsAllowHeaders = corsAllowHeaders;
        this.corsExposeHeaders = corsExposeHeaders;
        this.corsMaxAge = corsMaxAge;
        this.corsPrefligthErrorStatus = corsPrefligthErrorStatus;
        this.corsBlockIfUnauthorized = corsBlockIfUnauthorized;
        this.corsDefaultOptionsMethodsHandlePreflight = corsDefaultOptionsMethodsHandlePreflight;
    }

    Multibinder<Object> providersBinder;
    Multibinder<Interceptor> inInterceptorsBinder;
    Multibinder<Interceptor> outInterceptorsBinder;
    Multibinder<Interceptor> faultInterceptorsBinder;

    protected void configure() {
        providersBinder = Multibinder.newSetBinder(binder(), Object.class, CxfQualifiers.Providers.class);
        inInterceptorsBinder = Multibinder.newSetBinder(binder(), Interceptor.class, CxfQualifiers.InInterceptors.class);
        outInterceptorsBinder = Multibinder.newSetBinder(binder(), Interceptor.class, CxfQualifiers.OutInterceptors.class);
        faultInterceptorsBinder = Multibinder.newSetBinder(binder(), Interceptor.class, CxfQualifiers.FaultInterceptors.class);

        configureServer();
        if (exchangeIdInterceptors) {
            configureExchangeInterceptors();
        }
        configureOptions();
        configureClientExceptionMapper();
        configurePayloadMessageBodyWriter();
        configureCrossOriginResourceSharingFilter();
        configureSetDefaultContentTypePreMatchContainerRequestFilter();
        configureFaultInterceptor();
        configureJudoAuthorizingInterceptor();
        configureJacksonJaxbJsonProvider();
        configureISO8601DateParamHandler();
    }

    protected void configureOptions() {
        bind(Integer.class).annotatedWith(CxfConfigurations.CxfJaxRsServerPort.class).toInstance(cxfJaxRsServerPort);
        bind(String.class).annotatedWith(CxfConfigurations.CxfJaxRsServerUrl.class).toInstance(cxfJaxRsServerUrl);
        bind(String.class).annotatedWith(CxfConfigurations.CxfJaxRsServerPath.class).toInstance(cxfJaxRsServerPath);
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfSkipDefaultJsonProviderRegistration.class).toInstance(cxfSkipDefaultJsonProviderRegistration);
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfWadlServiceDescriptionAvailable.class).toInstance(cxfWadlServiceDescriptionAvailable);
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfMetricsEnabled.class).toInstance(cxfMetricsEnabled);
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfLoggingEnabled.class).toInstance(cxfLoggingEnabled);
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfLogException.class).toInstance(cxfLogException);
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfReturnRuntimeExceptions.class).toInstance(cxfReturnRuntimeExceptions);
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfIncludeBusinessCause.class).toInstance(cxfIncludeBusinessCause);
        bind(String.class).annotatedWith(CxfConfigurations.CxfDefaultRequestContentType.class).toInstance(cxfDefaultRequestContentType);
        bind(String.class).annotatedWith(CxfConfigurations.CxfCorsAllowOrigin.class).toInstance(corsAllowOrigin);
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfCorsAllowCredentials.class).toInstance(corsAllowCredentials);
        bind(String.class).annotatedWith(CxfConfigurations.CxfCorsAllowHeaders.class).toInstance(corsAllowHeaders);
        bind(String.class).annotatedWith(CxfConfigurations.CxfCorsExposeHeaders.class).toInstance(corsExposeHeaders);
        bind(Integer.class).annotatedWith(CxfConfigurations.CxfCorsMaxAge.class).toInstance(corsMaxAge);
        bind(Integer.class).annotatedWith(CxfConfigurations.CxfCorsPrefligthErrorStatus.class).toInstance(corsPrefligthErrorStatus);
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfCorsBlockIfUnauthorized.class).toInstance(corsBlockIfUnauthorized);
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfCorsDefaultOptionsMethodsHandlePreflight.class).toInstance(corsDefaultOptionsMethodsHandlePreflight);

    }

    protected void configureServer() {
        bind(CxfJaxrsServerProvider.ServerHolder.class).toProvider(CxfJaxrsServerProvider.class).in(Singleton.class);
    }

    protected void configureExchangeInterceptors() {
        inInterceptorsBinder.addBinding().toProvider(ExchangeIdDecoratorProvider.class).asEagerSingleton();
        outInterceptorsBinder.addBinding().toProvider(ExchangeIdResponseWriterProvider.class).asEagerSingleton();
    }

    protected void configureCrossOriginResourceSharingFilter() {
        providersBinder.addBinding().toProvider(CrossOriginResourceSharingFilterProvider.class).asEagerSingleton();
    }

    protected void configureClientExceptionMapper() {
        providersBinder.addBinding().toProvider(ClientExceptionMapperProvider.class).in(Singleton.class);
    }

    protected void configurePayloadMessageBodyWriter() {
        providersBinder.addBinding().toProvider(PayloadMessageBodyWriterProvider.class).in(Singleton.class);
    }

    protected void configureSetDefaultContentTypePreMatchContainerRequestFilter() {
        providersBinder.addBinding().toProvider(SetDefaultContentTypePreMatchContainerRequestFilterProvider.class).in(Singleton.class);
    }

    protected void configureFaultInterceptor() {
        faultInterceptorsBinder.addBinding().toProvider(FaultInterceptorProvider.class).in(Singleton.class);
    }

    protected void configureJudoAuthorizingInterceptor() {
        inInterceptorsBinder.addBinding().toProvider(JudoAuthorizingInterceptorProvider.class).in(Singleton.class);
    }

    protected void configureJacksonJaxbJsonProvider() {
        providersBinder.addBinding().toProvider(JacksonJaxbJsonProviderProvider.class).in(Singleton.class);
    }

    protected void configureISO8601DateParamHandler() {
        providersBinder.addBinding().toProvider(ISO8601DateParamHandlerProvider.class).in(Singleton.class);
    }

}
