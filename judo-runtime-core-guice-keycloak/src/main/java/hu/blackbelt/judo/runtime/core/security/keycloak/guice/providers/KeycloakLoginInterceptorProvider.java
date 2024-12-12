package hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.judo.runtime.core.security.RealmExtractor;
import hu.blackbelt.judo.runtime.core.security.keycloak.cxf.KeycloakLoginInterceptor;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;


public class KeycloakLoginInterceptorProvider implements Provider<KeycloakLoginInterceptor> {
    @Inject
    JudoModelLoader models;

    @Inject
    OpenIdConfigurationProvider openIdConfigurationProvider;

    @Inject
    RealmExtractor realmExtractor;

    @Inject
    TransformationTraceService transformationTraceService;

    public KeycloakLoginInterceptor get() {
        return KeycloakLoginInterceptor.builder()
                .asmModel(models.getAsmModel())
                .openIdConfigurationProvider(openIdConfigurationProvider)
                .realmExtractor(realmExtractor)
                .transformationTraceService(transformationTraceService)
                .build();
    }
}
