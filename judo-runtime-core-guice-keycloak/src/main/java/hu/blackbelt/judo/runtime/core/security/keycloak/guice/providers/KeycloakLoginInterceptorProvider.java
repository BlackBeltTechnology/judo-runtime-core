package hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.guice.JudoConfigurationQualifiers;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.security.AcceptableClientsParser;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleConfig;
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

    /**
     * JNG-6415 locale config — optional so a deployment that has not wired it (or has bound only
     * some of the four settings) still constructs a valid interceptor. When absent, the interceptor's
     * own {@code LocaleResolutionLevel} null-tolerance treats it as
     * {@link hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel#DEFAULT} (BROWSER),
     * matching the pre-refactor default. Fixes a pre-existing bug where the Guice provider never
     * set the toggle at all — see
     * {@code openspec/changes/replace-browser-check-with-resolution-level} D7.
     */
    @Inject(optional = true)
    @Nullable
    PrincipalLocaleConfig localeConfig = null;

    public KeycloakLoginInterceptor get() {
        final Map<String, String> clientToActorMap = AcceptableClientsParser.buildClientToActorMap(
                AcceptableClientsParser.parseAcceptableClients(acceptableClients));

        return KeycloakLoginInterceptor.builder()
                .asmModel(models.getAsmModel())
                .openIdConfigurationProvider(openIdConfigurationProvider)
                .realmExtractor(realmExtractor)
                .transformationTraceService(transformationTraceService)
                .clientToActorMap(clientToActorMap)
                .localeResolutionLevel(localeConfig != null ? localeConfig.getLocaleResolutionLevel() : null)
                .build();
    }
}
