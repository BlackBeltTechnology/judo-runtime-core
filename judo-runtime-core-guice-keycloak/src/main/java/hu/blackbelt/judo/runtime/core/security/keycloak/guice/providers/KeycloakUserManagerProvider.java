package hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers;

/*-
 * #%L
 * JUDO Services Keycloak Security
 * %%
 * Copyright (C) 2018 - 2023 BlackBelt Technology
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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.security.PasswordPolicy;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakAdminClient;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakUserManager;
import hu.blackbelt.judo.runtime.core.security.keycloak.guice.KeycloakConfigurationQualifiers;
import hu.blackbelt.judo.tatami.core.TransformationTrace;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import javax.annotation.Nullable;
import java.util.Objects;

public class KeycloakUserManagerProvider implements Provider<KeycloakUserManager> {

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakUserManagerEnabled
    @Nullable
    Boolean keycloakUserManagerEnabled;

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakUserManagerUpdateExistingUsers
    @Nullable
    Boolean keycloakUserManagerUpdateExistingUsers;

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakUserManagerRequiredActions
    @Nullable
    String keycloakUserManagerRequiredActions;

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakUserManagerAsyncServiceCall
    @Nullable
    Boolean keycloakUserManagerAsyncServiceCall;

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakUserManagerRetryMaxAttempts
    @Nullable
    Integer keycloakUserManagerRetryMaxAttempts;

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakUserManagerRetryExponentialBackoff
    @Nullable
    Boolean keycloakUserManagerRetryExponentialBackoff;

    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakUserManagerRetryWaitDuration
    @Nullable
    Long keycloakUserManagerRetryWaitDuration;

    @Inject
    JudoModelLoader judoModelLoader;

    @Inject
    PasswordPolicy defaultPasswordPolicy;

    @Inject
    TransformationTraceService transformationTraceService;

    @Inject
    KeycloakAdminClient keycloakAdminClient;
    
    public KeycloakUserManager get() {
        KeycloakUserManager keycloakUserManager = KeycloakUserManager.builder()
                .enabled(Objects.requireNonNullElse(keycloakUserManagerEnabled, true))
                .updateExistingUsers(Objects.requireNonNullElse(keycloakUserManagerUpdateExistingUsers,false))
                .requiredActions(Objects.requireNonNullElse(keycloakUserManagerRequiredActions, ""))
                .asyncServiceCall(Objects.requireNonNullElse(keycloakUserManagerAsyncServiceCall, true))
                .retryMaxAttempts(Objects.requireNonNullElse(keycloakUserManagerRetryMaxAttempts, 1000))
                .retryExponentialBackoff(Objects.requireNonNullElse(keycloakUserManagerRetryExponentialBackoff, true))
                .retryWaitDuration(Objects.requireNonNullElse(keycloakUserManagerRetryWaitDuration, 1000L))
                .asmModel(judoModelLoader.getAsmModel())
                .defaultPasswordPolicy(defaultPasswordPolicy)
                .transformationTrace(judoModelLoader.getAsm2keycloak())
                .transformationTraceService(transformationTraceService)
                .keycloakAdminClient(keycloakAdminClient)
                .build();
        return keycloakUserManager;
    }
}
