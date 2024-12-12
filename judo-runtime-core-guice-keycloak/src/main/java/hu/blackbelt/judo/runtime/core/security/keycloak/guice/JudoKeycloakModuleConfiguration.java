package hu.blackbelt.judo.runtime.core.security.keycloak.guice;

import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakUserManager;
import lombok.*;

import java.util.function.Consumer;

@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class JudoKeycloakModuleConfiguration {
    public static final JudoKeycloakModuleConfiguration DEFAULT = JudoKeycloakModuleConfiguration.builder().build();

    @Builder.Default
    String keycloakServerUrl = "http://localhost:8080/auth";

    @Builder.Default
    String keycloakPublicUrl = "http://localhost:8080/auth";

    @Builder.Default
    String keycloakAdminUser = "admin";

    @Builder.Default
    String keycloakAdminPassword = "judo";

    @Builder.Default
    String keycloakClientSecret = "";

    @Builder.Default
    Boolean keycloakRealmSynchronizerSupportLoginByEmail = true;

    @Builder.Default
    String keycloakRealmSynchronizerClientAccessTypeForHuman = "CONFIDENTIAL";

    @Builder.Default
    String keycloakRealmSynchronizerClientAccessTypeForSystem = "BEARER_ONLY";

    @Builder.Default
    String keycloakRealmSynchronizerCorsAllowOrigin = "*";

    @Builder.Default
    Boolean keycloakRealmSynchronizerAsyncServiceCall = true;

    @Builder.Default
    Integer keycloakRealmSynchronizerRetryMaxAttempts = 1000;

    @Builder.Default
    Boolean keycloakRealmSynchronizerRetryExponentialBackoff = true;;

    @Builder.Default
    Long keycloakRealmSynchronizerRetryWaitDuration = 1000L;

    @Builder.Default
    Consumer<KeycloakUserManager> keycloakIdentityManagerIsReady = (userManager) -> { userManager.setIdentityManagerReady(true); };

    @Builder.Default
    Boolean keycloakUserManagerEnabled = true;

    @Builder.Default
    Boolean keycloakUserManagerUpdateExistingUsers = false;

    @Builder.Default
    String keycloakUserManagerRequiredActions = "";

    @Builder.Default
    Boolean keycloakUserManagerAsyncServiceCall = true;

    @Builder.Default
    Integer keycloakUserManagerRetryMaxAttempts = 1000;

    @Builder.Default
    Boolean keycloakUserManagerRetryExponentialBackoff = true;

    @Builder.Default
    Long keycloakUserManagerRetryWaitDuration = 1000L;

    @Builder.Default
    String keycloakSecurityPasswordPolicyType = "NO_PASSWORD";

}
