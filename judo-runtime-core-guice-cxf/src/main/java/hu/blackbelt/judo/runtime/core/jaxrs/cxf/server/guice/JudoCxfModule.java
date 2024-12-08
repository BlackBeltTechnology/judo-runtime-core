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
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.providers.*;
import lombok.*;
import org.apache.cxf.interceptor.Interceptor;

public class JudoCxfModule extends AbstractModule {

    @Getter
    private JudoCxfModuleConfiguration configuration;

    public static class JudoCxfModuleBuilder {
        JudoCxfModuleConfiguration configuration = null;
        Boolean exchangeIdInterceptors = JudoCxfModuleConfiguration.DEFAULT.getExchangeIdInterceptors();
        String cxfJaxRsServerUrl = JudoCxfModuleConfiguration.DEFAULT.getCxfJaxRsServerUrl();
        String cxfJaxRsServerPath = JudoCxfModuleConfiguration.DEFAULT.getCxfJaxRsServerPath();
        Boolean cxfSkipDefaultJsonProviderRegistration = JudoCxfModuleConfiguration.DEFAULT.getCxfSkipDefaultJsonProviderRegistration();
        Boolean cxfWadlServiceDescriptionAvailable = JudoCxfModuleConfiguration.DEFAULT.getCxfWadlServiceDescriptionAvailable();
        Boolean cxfMetricsEnabled = JudoCxfModuleConfiguration.DEFAULT.getCxfMetricsEnabled();
        Boolean cxfLoggingEnabled = JudoCxfModuleConfiguration.DEFAULT.getCxfLoggingEnabled();
        Boolean cxfLogException = JudoCxfModuleConfiguration.DEFAULT.getCxfLogException();
        Boolean cxfReturnRuntimeExceptions = JudoCxfModuleConfiguration.DEFAULT.getCxfReturnRuntimeExceptions();
        Boolean cxfIncludeBusinessCause = JudoCxfModuleConfiguration.DEFAULT.getCxfIncludeBusinessCause();
        String cxfDefaultRequestContentType = JudoCxfModuleConfiguration.DEFAULT.getCxfDefaultRequestContentType();
        String corsAllowOrigin = JudoCxfModuleConfiguration.DEFAULT.getCorsAllowOrigin();
        Boolean corsAllowCredentials = JudoCxfModuleConfiguration.DEFAULT.getCorsAllowCredentials();
        String corsAllowHeaders = JudoCxfModuleConfiguration.DEFAULT.getCorsAllowHeaders();
        String corsExposeHeaders = JudoCxfModuleConfiguration.DEFAULT.getCorsExposeHeaders();
        Integer corsMaxAge = JudoCxfModuleConfiguration.DEFAULT.getCorsMaxAge();
        Integer corsPrefligthErrorStatus = JudoCxfModuleConfiguration.DEFAULT.getCorsPrefligthErrorStatus();
        Boolean corsBlockIfUnauthorized = JudoCxfModuleConfiguration.DEFAULT.getCorsBlockIfUnauthorized();
        Boolean corsDefaultOptionsMethodsHandlePreflight = JudoCxfModuleConfiguration.DEFAULT.getCorsDefaultOptionsMethodsHandlePreflight();
    }



    @Builder
    private JudoCxfModule(JudoCxfModuleConfiguration configuration,
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
        if (configuration != null) {
            this.configuration = configuration;
        } else {
            this.configuration = JudoCxfModuleConfiguration.builder()
                    .cxfJaxRsServerUrl(cxfJaxRsServerUrl)
                    .cxfJaxRsServerPath(cxfJaxRsServerPath)
                    .exchangeIdInterceptors(exchangeIdInterceptors)
                    .cxfSkipDefaultJsonProviderRegistration(cxfSkipDefaultJsonProviderRegistration)
                    .cxfWadlServiceDescriptionAvailable(cxfWadlServiceDescriptionAvailable)
                    .cxfMetricsEnabled(cxfMetricsEnabled)
                    .cxfLoggingEnabled(cxfLoggingEnabled)
                    .cxfLogException(cxfLogException)
                    .cxfReturnRuntimeExceptions(cxfReturnRuntimeExceptions)
                    .cxfIncludeBusinessCause(cxfIncludeBusinessCause)
                    .cxfDefaultRequestContentType(cxfDefaultRequestContentType)
                    .corsAllowOrigin(corsAllowOrigin)
                    .corsAllowCredentials(corsAllowCredentials)
                    .corsAllowHeaders(corsAllowHeaders)
                    .corsExposeHeaders(corsExposeHeaders)
                    .corsMaxAge(corsMaxAge)
                    .corsPrefligthErrorStatus(corsPrefligthErrorStatus)
                    .corsBlockIfUnauthorized(corsBlockIfUnauthorized)
                    .corsDefaultOptionsMethodsHandlePreflight(corsDefaultOptionsMethodsHandlePreflight)
                    .build();
        }
    }

    Multibinder<Object> providersBinder;
    Multibinder<Interceptor> inInterceptorsBinder;
    Multibinder<Interceptor> outInterceptorsBinder;
    Multibinder<Interceptor> faultInterceptorsBinder;

    @Inject
    @Getter
    CxfJaxrsServerProvider.ServerHolder serverHolder;

    protected void configure() {
        providersBinder = Multibinder.newSetBinder(binder(), Object.class, CxfQualifiers.Providers.class);
        inInterceptorsBinder = Multibinder.newSetBinder(binder(), Interceptor.class, CxfQualifiers.InInterceptors.class);
        outInterceptorsBinder = Multibinder.newSetBinder(binder(), Interceptor.class, CxfQualifiers.OutInterceptors.class);
        faultInterceptorsBinder = Multibinder.newSetBinder(binder(), Interceptor.class, CxfQualifiers.FaultInterceptors.class);

        configureServer();
        if (configuration.getExchangeIdInterceptors()) {
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
        bind(String.class).annotatedWith(CxfConfigurations.CxfJaxRsServerUrl.class).toInstance(configuration.getCxfJaxRsServerUrl());
        bind(String.class).annotatedWith(CxfConfigurations.CxfJaxRsServerPath.class).toInstance(configuration.getCxfJaxRsServerPath());
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfSkipDefaultJsonProviderRegistration.class).toInstance(configuration.getCxfSkipDefaultJsonProviderRegistration());
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfWadlServiceDescriptionAvailable.class).toInstance(configuration.getCxfWadlServiceDescriptionAvailable());
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfMetricsEnabled.class).toInstance(configuration.getCxfMetricsEnabled());
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfLoggingEnabled.class).toInstance(configuration.getCxfLoggingEnabled());
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfLogException.class).toInstance(configuration.getCxfLogException());
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfReturnRuntimeExceptions.class).toInstance(configuration.getCxfReturnRuntimeExceptions());
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfIncludeBusinessCause.class).toInstance(configuration.getCxfIncludeBusinessCause());
        bind(String.class).annotatedWith(CxfConfigurations.CxfDefaultRequestContentType.class).toInstance(configuration.getCxfDefaultRequestContentType());
        bind(String.class).annotatedWith(CxfConfigurations.CxfCorsAllowOrigin.class).toInstance(configuration.getCorsAllowOrigin());
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfCorsAllowCredentials.class).toInstance(configuration.getCorsAllowCredentials());
        bind(String.class).annotatedWith(CxfConfigurations.CxfCorsAllowHeaders.class).toInstance(configuration.getCorsAllowHeaders());
        bind(String.class).annotatedWith(CxfConfigurations.CxfCorsExposeHeaders.class).toInstance(configuration.getCorsExposeHeaders());
        bind(Integer.class).annotatedWith(CxfConfigurations.CxfCorsMaxAge.class).toInstance(configuration.getCorsMaxAge());
        bind(Integer.class).annotatedWith(CxfConfigurations.CxfCorsPrefligthErrorStatus.class).toInstance(configuration.getCorsPrefligthErrorStatus());
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfCorsBlockIfUnauthorized.class).toInstance(configuration.getCorsBlockIfUnauthorized());
        bind(Boolean.class).annotatedWith(CxfConfigurations.CxfCorsDefaultOptionsMethodsHandlePreflight.class).toInstance(configuration.getCorsDefaultOptionsMethodsHandlePreflight());
    }

    protected void configureServer() {
        bind(CxfJaxrsServerProvider.ServerHolder.class).toProvider(CxfJaxrsServerProvider.class).asEagerSingleton();
    }

    protected void configureExchangeInterceptors() {
        inInterceptorsBinder.addBinding().toProvider(ExchangeIdDecoratorProvider.class).asEagerSingleton();
        outInterceptorsBinder.addBinding().toProvider(ExchangeIdResponseWriterProviderOut.class).asEagerSingleton();
        faultInterceptorsBinder.addBinding().toProvider(ExchangeIdResponseWriterProviderFault.class).asEagerSingleton();
    }

    protected void configureCrossOriginResourceSharingFilter() {
        providersBinder.addBinding().toProvider(CrossOriginResourceSharingFilterProvider.class).asEagerSingleton();
    }

    protected void configureClientExceptionMapper() {
        providersBinder.addBinding().toProvider(ClientExceptionMapperProvider.class).asEagerSingleton();
    }

    protected void configurePayloadMessageBodyWriter() {
        providersBinder.addBinding().toProvider(PayloadMessageBodyWriterProvider.class).asEagerSingleton();
    }

    protected void configureSetDefaultContentTypePreMatchContainerRequestFilter() {
        providersBinder.addBinding().toProvider(SetDefaultContentTypePreMatchContainerRequestFilterProvider.class).asEagerSingleton();
    }

    protected void configureFaultInterceptor() {
        faultInterceptorsBinder.addBinding().toProvider(FaultInterceptorProvider.class).asEagerSingleton();
    }

    protected void configureJudoAuthorizingInterceptor() {
        inInterceptorsBinder.addBinding().toProvider(JudoAuthorizingInterceptorProvider.class).asEagerSingleton();
    }

    protected void configureJacksonJaxbJsonProvider() {
        providersBinder.addBinding().toProvider(JacksonJaxbJsonProviderProvider.class).asEagerSingleton();
    }

    protected void configureISO8601DateParamHandler() {
        providersBinder.addBinding().toProvider(ISO8601DateParamHandlerProvider.class).asEagerSingleton();
    }

}
