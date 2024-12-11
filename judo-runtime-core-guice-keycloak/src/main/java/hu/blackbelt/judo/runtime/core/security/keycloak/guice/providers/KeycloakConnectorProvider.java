package hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakConnector;
import hu.blackbelt.judo.runtime.core.security.keycloak.guice.KeycloakConfigurationQualifiers;

import javax.annotation.Nullable;

public class KeycloakConnectorProvider implements Provider<KeycloakConnector> {

    @Inject
    AsmModel asmModel;

    @Inject
    ObjectMapper objectMapper;

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakServerUrl
    @Nullable
    String keycloakServerUrl = "http://localhost:8080";

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakPublicUrl
    @Nullable
    String keycloakPublicUrl = "http://localhost:8080";

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakAdminUser
    @Nullable
    String keycloakAdminUser = "admin";

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakAdminPassword
    @Nullable
    String keycloakAdminPassword = "admin";

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakClientSecret
    @Nullable
    String keycloakClientSecret = "";

    @Override
    public KeycloakConnector get() {
        KeycloakConnector keycloakConnector = KeycloakConnector.builder()
                .serverUrl(keycloakServerUrl)
                .externalUrl(keycloakPublicUrl)
                .adminUser(keycloakAdminUser)
                .adminPassword(keycloakAdminPassword)
                .clientSecret(keycloakClientSecret)
                .asmModel(asmModel)
                .objectMapper(objectMapper)
                .build();
        return keycloakConnector;
    }
}
