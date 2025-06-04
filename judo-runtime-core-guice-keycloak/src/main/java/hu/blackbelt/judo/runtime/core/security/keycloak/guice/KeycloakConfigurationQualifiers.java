package hu.blackbelt.judo.runtime.core.security.keycloak.guice;

import com.google.inject.BindingAnnotation;

import javax.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class KeycloakConfigurationQualifiers {

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakServerUrl {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakPublicUrl {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakAdminUser {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakAdminPassword {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakClientSecret {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakRealmSynchronizerSupportLoginByEmail {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakRealmSynchronizerClientAccessTypeForHuman {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakRealmSynchronizerClientAccessTypeForSystem {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakRealmSynchronizerCorsAllowOrigin {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakRealmSynchronizerAsyncServiceCall {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakRealmSynchronizerRetryMaxAttempts {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakRealmSynchronizerRetryExponentialBackoff {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakRealmSynchronizerRetryWaitDuration {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakIdentityManagerIsReady {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakUserManagerEnabled {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakUserManagerUpdateExistingUsers {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakUserManagerRequiredActions {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakUserManagerAsyncServiceCall {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakUserManagerRetryMaxAttempts {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakUserManagerRetryExponentialBackoff {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakUserManagerRetryWaitDuration {}

    @Qualifier
    @Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    @BindingAnnotation
    public @interface KeycloakSecurityPasswordPolicyType {}



}
