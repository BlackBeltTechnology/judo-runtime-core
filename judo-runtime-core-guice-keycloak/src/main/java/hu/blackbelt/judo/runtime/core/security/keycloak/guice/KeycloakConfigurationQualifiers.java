package hu.blackbelt.judo.runtime.core.security.keycloak.guice;

import javax.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public class KeycloakConfigurationQualifiers {

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakServerUrl {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakPublicUrl {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakAdminUser {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakAdminPassword {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakClientSecret {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakRealmSynchronizerSupportLoginByEmail {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakRealmSynchronizerClientAccessTypeForHuman {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakRealmSynchronizerClientAccessTypeForSystem {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakRealmSynchronizerCorsAllowOrigin {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakRealmSynchronizerAsyncServiceCall {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakRealmSynchronizerRetryMaxAttempts {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakRealmSynchronizerRetryExponentialBackoff {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakRealmSynchronizerRetryWaitDuration {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakIdentityManagerIsReady {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakUserManagerEnabled {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakUserManagerUpdateExistingUsers {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakUserManagerRequiredActions {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakUserManagerAsyncServiceCall {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakUserManagerRetryMaxAttempts {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakUserManagerRetryExponentialBackoff {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakUserManagerRetryWaitDuration {}

    @Qualifier
    @Retention(RetentionPolicy.RUNTIME)
    public @interface KeycloakSecurityPasswordPolicyType {}



}
