package hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakConnector;

public class KeycloakConnectorOpenIdConfigurationProviderProvider implements Provider<OpenIdConfigurationProvider> {

    @Inject
    KeycloakConnector keycloakConnector;

    @Override
    public OpenIdConfigurationProvider get() {
        return keycloakConnector;
    }
}
