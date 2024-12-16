package hu.blackbelt.judo.runtime.core.security.keycloak.guice;

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
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import hu.blackbelt.judo.runtime.core.guice.security.PathInfoRealmExtractorProvider;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice.CxfQualifiers;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.judo.runtime.core.security.PasswordPolicy;
import hu.blackbelt.judo.runtime.core.security.RealmExtractor;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakAdminClient;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakConnector;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakRealmSynchronizer;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakUserManager;
import hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers.*;
import hu.blackbelt.judo.runtime.core.utils.RuntimeVariableResolver;
import lombok.Builder;
import org.apache.cxf.interceptor.Interceptor;

import java.util.Objects;
import java.util.function.Consumer;

public class JudoKeycloakModule extends AbstractModule {

    JudoKeycloakModuleConfiguration configuration;

    public static class JudoKeycloakModuleBuilder {
        JudoKeycloakModuleConfiguration configuration = null;
        RuntimeVariableResolver runtimeVariableResolver = null;
        String keycloakServerUrl = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakServerUrl();
        String keycloakPublicUrl = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakPublicUrl();
        String keycloakAdminUser = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakAdminUser();
        String keycloakAdminPassword = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakAdminPassword();
        String keycloakClientSecret = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakClientSecret();
        Boolean keycloakRealmSynchronizerSupportLoginByEmail = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakRealmSynchronizerSupportLoginByEmail();
        String keycloakRealmSynchronizerClientAccessTypeForHuman = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakRealmSynchronizerClientAccessTypeForHuman();
        String keycloakRealmSynchronizerClientAccessTypeForSystem = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakRealmSynchronizerClientAccessTypeForSystem();
        String keycloakRealmSynchronizerCorsAllowOrigin = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakRealmSynchronizerCorsAllowOrigin();
        Boolean keycloakRealmSynchronizerAsyncServiceCall = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakRealmSynchronizerAsyncServiceCall();
        Integer keycloakRealmSynchronizerRetryMaxAttempts = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakRealmSynchronizerRetryMaxAttempts();
        Boolean keycloakRealmSynchronizerRetryExponentialBackoff = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakRealmSynchronizerRetryExponentialBackoff();
        Long keycloakRealmSynchronizerRetryWaitDuration = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakRealmSynchronizerRetryWaitDuration();
        Consumer<KeycloakUserManager> keycloakIdentityManagerIsReady = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakIdentityManagerIsReady();
        Boolean keycloakUserManagerEnabled = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakUserManagerEnabled();
        Boolean keycloakUserManagerUpdateExistingUsers = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakUserManagerUpdateExistingUsers();
        String keycloakUserManagerRequiredActions = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakUserManagerRequiredActions();
        Boolean keycloakUserManagerAsyncServiceCall = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakUserManagerAsyncServiceCall();
        Integer keycloakUserManagerRetryMaxAttempts = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakUserManagerRetryMaxAttempts();
        Boolean keycloakUserManagerRetryExponentialBackoff = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakUserManagerRetryExponentialBackoff();
        Long keycloakUserManagerRetryWaitDuration = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakUserManagerRetryWaitDuration();
        String keycloakSecurityPasswordPolicyType = JudoKeycloakModuleConfiguration.DEFAULT.getKeycloakSecurityPasswordPolicyType();
    }

    @Builder
    public JudoKeycloakModule(JudoKeycloakModuleConfiguration configuration,
                                    RuntimeVariableResolver runtimeVariableResolver,
                                    String keycloakServerUrl,
                                    String keycloakPublicUrl,
                                    String keycloakAdminUser,
                                    String keycloakAdminPassword,
                                    String keycloakClientSecret,
                                    Boolean keycloakRealmSynchronizerSupportLoginByEmail,
                                    String keycloakRealmSynchronizerClientAccessTypeForHuman,
                                    String keycloakRealmSynchronizerClientAccessTypeForSystem,
                                    String keycloakRealmSynchronizerCorsAllowOrigin,
                                    Boolean keycloakRealmSynchronizerAsyncServiceCall,
                                    Integer keycloakRealmSynchronizerRetryMaxAttempts,
                                    Boolean keycloakRealmSynchronizerRetryExponentialBackoff,
                                    Long keycloakRealmSynchronizerRetryWaitDuration,
                                    Consumer<KeycloakUserManager> keycloakIdentityManagerIsReady,
                                    Boolean keycloakUserManagerEnabled,
                                    Boolean keycloakUserManagerUpdateExistingUsers,
                                    String keycloakUserManagerRequiredActions,
                                    Boolean keycloakUserManagerAsyncServiceCall,
                                    Integer keycloakUserManagerRetryMaxAttempts,
                                    Boolean keycloakUserManagerRetryExponentialBackoff,
                                    Long keycloakUserManagerRetryWaitDuration,
                                    String keycloakSecurityPasswordPolicyType
                              ) {

        if (configuration != null) {
            this.configuration = configuration;
        } else {
            this.configuration = JudoKeycloakModuleConfiguration.builder()
                    .runtimeVariableResolver(Objects.requireNonNullElseGet(runtimeVariableResolver,
                            () -> RuntimeVariableResolver.builder().prefix("judo").build()))
                    .keycloakServerUrl(keycloakServerUrl)
                    .keycloakPublicUrl(keycloakPublicUrl)
                    .keycloakAdminUser(keycloakAdminUser)
                    .keycloakAdminPassword(keycloakAdminPassword)
                    .keycloakClientSecret(keycloakClientSecret)
                    .keycloakRealmSynchronizerSupportLoginByEmail(keycloakRealmSynchronizerSupportLoginByEmail)
                    .keycloakRealmSynchronizerClientAccessTypeForHuman(keycloakRealmSynchronizerClientAccessTypeForHuman)
                    .keycloakRealmSynchronizerClientAccessTypeForSystem(keycloakRealmSynchronizerClientAccessTypeForSystem)
                    .keycloakRealmSynchronizerCorsAllowOrigin(keycloakRealmSynchronizerCorsAllowOrigin)
                    .keycloakRealmSynchronizerAsyncServiceCall(keycloakRealmSynchronizerAsyncServiceCall)
                    .keycloakRealmSynchronizerRetryMaxAttempts(keycloakRealmSynchronizerRetryMaxAttempts)
                    .keycloakRealmSynchronizerRetryExponentialBackoff(keycloakRealmSynchronizerRetryExponentialBackoff)
                    .keycloakRealmSynchronizerRetryWaitDuration(keycloakRealmSynchronizerRetryWaitDuration)
                    .keycloakIdentityManagerIsReady(keycloakIdentityManagerIsReady)
                    .keycloakUserManagerEnabled(keycloakUserManagerEnabled)
                    .keycloakUserManagerUpdateExistingUsers(keycloakUserManagerUpdateExistingUsers)
                    .keycloakUserManagerRequiredActions(keycloakUserManagerRequiredActions)
                    .keycloakUserManagerAsyncServiceCall(keycloakUserManagerAsyncServiceCall)
                    .keycloakUserManagerRetryMaxAttempts(keycloakUserManagerRetryMaxAttempts)
                    .keycloakUserManagerRetryExponentialBackoff(keycloakUserManagerRetryExponentialBackoff)
                    .keycloakUserManagerRetryWaitDuration(keycloakUserManagerRetryWaitDuration)
                    .keycloakSecurityPasswordPolicyType(keycloakSecurityPasswordPolicyType)
                    .build();
        }
    }

    protected void configure() {
        configureKeycloakLoginInterceptor();
        configurePasswordPolicy();
        configureAdminClient();
        configureKeycloakConnector();
        configureKeycloakUserManager();
        configureRealmSyncornizer();
        configureRealmExtractor();
        configureAdditional();
        configureOptions();
    }

    public void configureAdditional() {
    }

    public void configureOptions() {
        bind(Consumer.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakIdentityManagerIsReady.class)
                .toInstance(configuration.getKeycloakIdentityManagerIsReady());

        bind(String.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakServerUrl.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsString("keycloakServerUrl",
                                configuration.getKeycloakServerUrl()));

        bind(String.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakPublicUrl.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsString("keycloakPublicUrl",
                                configuration.getKeycloakPublicUrl()));

        bind(String.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakAdminUser.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsString("keycloakAdminUser",
                                configuration.getKeycloakAdminUser()));

        bind(String.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakAdminPassword.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsString("keycloakAdminPassword",
                                configuration.getKeycloakAdminPassword()));

        bind(String.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakClientSecret.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsString("keycloakClientSecret",
                                configuration.getKeycloakClientSecret()));

        bind(Boolean.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakRealmSynchronizerSupportLoginByEmail.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsBoolean("keycloakRealmSynchronizerSupportLoginByEmail",
                                configuration.getKeycloakRealmSynchronizerSupportLoginByEmail()));

        bind(String.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakRealmSynchronizerClientAccessTypeForHuman.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsString("keycloakRealmSynchronizerClientAccessTypeForHuman",
                                configuration.getKeycloakRealmSynchronizerClientAccessTypeForHuman()));

        bind(String.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakRealmSynchronizerClientAccessTypeForSystem.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsString("keycloakRealmSynchronizerClientAccessTypeForSystem",
                                configuration.getKeycloakRealmSynchronizerClientAccessTypeForSystem()));

        bind(String.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakRealmSynchronizerCorsAllowOrigin.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsString("keycloakRealmSynchronizerCorsAllowOrigin",
                                configuration.getKeycloakRealmSynchronizerCorsAllowOrigin()));

        bind(Boolean.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakRealmSynchronizerAsyncServiceCall.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsBoolean("keycloakRealmSynchronizerAsyncServiceCall",
                                configuration.getKeycloakRealmSynchronizerAsyncServiceCall()));

        bind(Integer.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakRealmSynchronizerRetryMaxAttempts.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsInteger("keycloakRealmSynchronizerRetryMaxAttempts",
                                configuration.getKeycloakRealmSynchronizerRetryMaxAttempts()));

        bind(Boolean.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakRealmSynchronizerRetryExponentialBackoff.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsBoolean("keycloakRealmSynchronizerRetryExponentialBackoff",
                                configuration.getKeycloakRealmSynchronizerRetryExponentialBackoff()));

        bind(Long.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakRealmSynchronizerRetryWaitDuration.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsLong("keycloakRealmSynchronizerRetryWaitDuration",
                                configuration.getKeycloakRealmSynchronizerRetryWaitDuration()));

        bind(Boolean.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakUserManagerEnabled.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsBoolean("keycloakUserManagerEnabled",
                                configuration.getKeycloakUserManagerEnabled()));

        bind(Boolean.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakUserManagerUpdateExistingUsers.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsBoolean("keycloakUserManagerUpdateExistingUsers",
                                configuration.getKeycloakUserManagerUpdateExistingUsers()));

        bind(String.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakUserManagerRequiredActions.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsString("keycloakUserManagerRequiredActions",
                                configuration.getKeycloakUserManagerRequiredActions()));

        bind(Boolean.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakUserManagerAsyncServiceCall.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsBoolean("keycloakUserManagerAsyncServiceCall",
                                configuration.getKeycloakUserManagerAsyncServiceCall()));

        bind(Integer.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakUserManagerRetryMaxAttempts.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsInteger("keycloakUserManagerRetryMaxAttempts",
                                configuration.getKeycloakUserManagerRetryMaxAttempts()));

        bind(Boolean.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakUserManagerRetryExponentialBackoff.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsBoolean("keycloakUserManagerRetryExponentialBackoff",
                                configuration.getKeycloakUserManagerRetryExponentialBackoff()));

        bind(Long.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakUserManagerRetryWaitDuration.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsLong("keycloakUserManagerRetryWaitDuration",
                                configuration.getKeycloakUserManagerRetryWaitDuration()));

        bind(String.class).annotatedWith(KeycloakConfigurationQualifiers.KeycloakSecurityPasswordPolicyType.class)
                .toInstance(configuration.getRuntimeVariableResolver()
                        .getVariableAsString("keycloakSecurityPasswordPolicyType",
                                configuration.getKeycloakSecurityPasswordPolicyType()));
    }

    protected void configureKeycloakLoginInterceptor() {
        Multibinder<Interceptor> inInterceptorsBinder = Multibinder.newSetBinder(binder(), Interceptor.class, CxfQualifiers.InInterceptors.class);
        inInterceptorsBinder.addBinding().toProvider(KeycloakLoginInterceptorProvider.class).asEagerSingleton();
    }

    protected void configurePasswordPolicy() {
        bind(PasswordPolicy.class).toProvider(KeycloakPasswordPolicyProvider.class).asEagerSingleton();
    }

    protected void configureAdminClient() {
        bind(KeycloakAdminClient.class).toProvider(KeycloakAdminClientProvider.class).asEagerSingleton();
    }

    protected void configureKeycloakConnector() {
        bind(KeycloakConnector.class).toProvider(KeycloakConnectorProvider.class).asEagerSingleton();
        bind(OpenIdConfigurationProvider.class).toProvider(KeycloakConnectorOpenIdConfigurationProviderProvider.class).asEagerSingleton();
    }

    protected void configureKeycloakUserManager() {
        bind(KeycloakUserManager.class).toProvider(KeycloakUserManagerProvider.class).asEagerSingleton();
    }

    protected void configureRealmSyncornizer() {
        bind(KeycloakRealmSynchronizer.class).toProvider(KeycloakRealmSynchronizerProvider.class).asEagerSingleton();
    }

    protected void configureRealmExtractor() {
        bind(RealmExtractor.class).toProvider(PathInfoRealmExtractorProvider.class).asEagerSingleton();
    }

}
