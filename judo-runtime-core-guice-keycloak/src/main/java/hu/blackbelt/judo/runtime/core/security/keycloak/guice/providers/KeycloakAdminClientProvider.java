package hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakAdminClient;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakConnector;

public class KeycloakAdminClientProvider implements Provider<KeycloakAdminClient> {
    @Inject
    KeycloakConnector keycloakConnector;

    @Override
    public KeycloakAdminClient get() {
        return KeycloakAdminClient.builder()
                .keycloakConnector(keycloakConnector)
                .build();
    }
}
