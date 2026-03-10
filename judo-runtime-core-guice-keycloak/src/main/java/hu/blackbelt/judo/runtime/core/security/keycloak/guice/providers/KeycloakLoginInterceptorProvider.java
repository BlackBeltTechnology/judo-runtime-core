package hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.guice.JudoConfigurationQualifiers;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.security.AcceptableClientsParser;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.judo.runtime.core.security.RealmExtractor;
import hu.blackbelt.judo.runtime.core.security.keycloak.cxf.KeycloakLoginInterceptor;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;

import javax.annotation.Nullable;
import java.util.Map;


public class KeycloakLoginInterceptorProvider implements Provider<KeycloakLoginInterceptor> {
    @Inject
    JudoModelLoader models;

    @Inject
    OpenIdConfigurationProvider openIdConfigurationProvider;

    @Inject
    RealmExtractor realmExtractor;

    @Inject
    TransformationTraceService transformationTraceService;

    @Inject(optional = true)
    @JudoConfigurationQualifiers.ActorResolverAcceptableClients
    @Nullable
    String acceptableClients = null;

    public KeycloakLoginInterceptor get() {
        final Map<String, String> clientToActorMap = AcceptableClientsParser.buildClientToActorMap(
                AcceptableClientsParser.parseAcceptableClients(acceptableClients));

        return KeycloakLoginInterceptor.builder()
                .asmModel(models.getAsmModel())
                .openIdConfigurationProvider(openIdConfigurationProvider)
                .realmExtractor(realmExtractor)
                .transformationTraceService(transformationTraceService)
                .clientToActorMap(clientToActorMap)
                .build();
    }
}
