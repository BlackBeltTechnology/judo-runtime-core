package hu.blackbelt.judo.runtime.core.security.keycloak;

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

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.keycloak.Client;
import hu.blackbelt.judo.meta.keycloak.Realm;
import hu.blackbelt.judo.meta.keycloak.runtime.KeycloakModel;
import hu.blackbelt.judo.meta.keycloak.runtime.KeycloakUtils;
import hu.blackbelt.judo.tatami.core.TransformationTrace;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.structured.map.proxy.MapProxy;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.ecore.EClass;

import java.util.*;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class KeycloakRealmSynchronizer {

    public enum AccessType {
        PUBLIC, CONFIDENTIAL, BEARER_ONLY
    }

    /*
    @ObjectClassDefinition
    public @interface Config {

        @AttributeDefinition(name = "Support login by email", type = AttributeType.BOOLEAN)
        boolean realm_loginByEmail() default true;

        @AttributeDefinition(name = "Default access type for humans")
        String client_accessType_human() default "CONFIDENTIAL";

        @AttributeDefinition(name = "Default access type for systems")
        String client_accessType_system() default "BEARER_ONLY";

        @AttributeDefinition(name = "CORS allow origin", description = "Comma-separated list of Access-Control-Allow-Origin")
        String cors_allowOrigin();

        @AttributeDefinition(name = "Asynchronous call of server with fault tolerance ")
        boolean asyncServiceCall() default true;

        @AttributeDefinition(name = "Maximum number of attempts.")
        int retryMaxAttempts() default 1000;

        @AttributeDefinition(name = "Use exponential backoff")
        boolean retryExponentialBackoff() default true;

        @AttributeDefinition(name = "Duration wait milliseconds between retry attempts. With the exponential backoff the inital wait duration")
        long retryWaitDuration() default 1000L;
    }
    */

    private KeycloakAdminClient keycloakAdminClient;
    private KeycloakModel keycloakModel;
    private TransformationTraceService transformationTraceService;
    private TransformationTrace transformationTrace;

    private AccessType systemDefaultAccessType = AccessType.CONFIDENTIAL;
    private AccessType humanDefaultSystemAccessType = AccessType.BEARER_ONLY;
    private Boolean supportLoginByEmail = true;
    private Collection<String> corsAllowOrigin = Collections.emptySet();
    private Boolean asyncServiceCall = true;
    private Integer retryMaxAttempts = 1000;
    private Boolean retryExponentialBackoff = true;

    private Long retryWaitDuration = 1000L;

    private Runnable registerIdentityManagerReady;

    private RetryRegistry retryRegistry;


    @Builder
    public KeycloakRealmSynchronizer(
            @NonNull KeycloakAdminClient keycloakAdminClient,
            @NonNull KeycloakModel keycloakModel,
            @NonNull TransformationTraceService transformationTraceService,
            @NonNull TransformationTrace transformationTrace,
            AccessType systemDefaultAccessType,
            AccessType humanDefaultSystemAccessType,
            Boolean supportLoginByEmail,
            Collection<String> corsAllowOrigin,
            Boolean asyncServiceCall,
            Integer retryMaxAttempts,
            Boolean retryExponentialBackoff,
            Long retryWaitDuration,
            Runnable registerIdentityManagerReady) {

        this.keycloakAdminClient = keycloakAdminClient;
        this.keycloakModel = keycloakModel;
        this.transformationTraceService = transformationTraceService;
        this.transformationTrace = transformationTrace;
        this.systemDefaultAccessType = Optional.ofNullable(systemDefaultAccessType).orElse(this.systemDefaultAccessType);
        this.humanDefaultSystemAccessType = Optional.ofNullable(humanDefaultSystemAccessType).orElse(this.humanDefaultSystemAccessType);
        this.supportLoginByEmail = Optional.ofNullable(supportLoginByEmail).orElse(this.supportLoginByEmail);
        this.corsAllowOrigin = Optional.ofNullable(corsAllowOrigin).orElse(this.corsAllowOrigin);
        this.asyncServiceCall = Optional.ofNullable(asyncServiceCall).orElse(this.asyncServiceCall);
        this.retryMaxAttempts = Optional.ofNullable(retryMaxAttempts).orElse(this.retryMaxAttempts);
        this.retryExponentialBackoff = Optional.ofNullable(retryExponentialBackoff).orElse(this.retryExponentialBackoff);
        this.retryWaitDuration = Optional.ofNullable(retryWaitDuration).orElse(this.retryWaitDuration);
        this.registerIdentityManagerReady = Optional.ofNullable(registerIdentityManagerReady).orElse(this.registerIdentityManagerReady);
    }

    public void synchronizeAllRealms() {
        KeycloakUtils keycloakUtils = new KeycloakUtils(keycloakModel.getResourceSet());

        retryRegistry = RetryUtil.createRetryRegistry(retryMaxAttempts,
                retryWaitDuration, retryExponentialBackoff);

        Retry retry = retryRegistry.retry("synchronizeAllRealms");
        Runnable task = Retry.decorateRunnable(retry, synchronizeAllRealmsCall());
        RetryUtil.registerLogEventHandlers(retry);

        if (asyncServiceCall) {
            CompletableFuture.runAsync(task).whenComplete((v, e) -> {
                if (e != null) {
                    log.error("Could not synchronize realms", e);
                } else {
                    if (registerIdentityManagerReady != null) {
                        registerIdentityManagerReady.run();
                    }
                }
            });
        } else {
            task.run();
            if (registerIdentityManagerReady != null) {
                registerIdentityManagerReady.run();
            }
        }
    }

    private Runnable synchronizeAllRealmsCall() {
        return () -> new KeycloakUtils(keycloakModel.getResourceSet()).all(Realm.class).forEach(realm -> synchronizeRealm(realm, keycloakAdminClient.getListOfRealms()));
    }


    private void synchronizeRealm(final Realm realm, final List<Realm> existingRealms) {
        if (log.isDebugEnabled()) {
            log.debug("Synchronizing realm: {}", realm.getRealm());
        }
        final Optional<Realm> existingRealm = existingRealms.stream().filter(r -> Objects.equals(r.getRealm(), realm.getRealm())).findAny();
        final Realm realmData = MapProxy.builder(Realm.class).newInstance();
        if (!existingRealm.isPresent()) {
            realmData.setId(realm.getId());
            realmData.setRealm(realm.getRealm());
            realmData.setEnabled(realm.getEnabled());
            realmData.setLoginWithEmailAllowed(realm.getLoginWithEmailAllowed() != null ? realm.getLoginWithEmailAllowed() : supportLoginByEmail);
            keycloakAdminClient.createOrUpdateRealm(realmData, false);
        } else {
            boolean dirty = false;
            realmData.setId(existingRealm.get().getId());
            realmData.setRealm(realm.getRealm());

            if (realm.getEnabled() != null && !Objects.equals(realm.getEnabled(), existingRealm.get().getEnabled())) {
                dirty = true;
                realmData.setEnabled(realm.getEnabled());
            }
            if (realm.getLoginWithEmailAllowed() != null && !Objects.equals(realm.getLoginWithEmailAllowed(), existingRealm.get().getLoginWithEmailAllowed())) {
                dirty = true;
                realmData.setLoginWithEmailAllowed(realm.getLoginWithEmailAllowed());
            }

            if (dirty) {
                keycloakAdminClient.createOrUpdateRealm(realmData, true);
            }
        }

        realm.getClients().forEach(client -> synchronizeClient(realm, client,
                keycloakAdminClient.getClientsOfRealm(realm.getRealm())));
    }

    private void synchronizeClient(final Realm realm, final Client client, final List<Client> existingClients) {
        if (log.isDebugEnabled()) {
            log.debug("Synchronizing client: {}", client.getName());
        }
        final Optional<Client> existingClient = existingClients.stream().filter(c -> Objects.equals(c.getName(), client.getName())).findAny();
        final Client clientData;
        if (!existingClient.isPresent()) {
            clientData = MapProxy.builder(Client.class).withMap(ImmutableMap.of(
                    "redirectUris", new BasicEList(),
                    "webOrigins", new BasicEList()
            )).newInstance();

            clientData.setClientId(client.getName());
            clientData.setName(client.getName());
            clientData.setEnabled(client.getEnabled());
            clientData.setDirectAccessGrantsEnabled(client.getDirectAccessGrantsEnabled());

            final boolean humanActor = Optional.ofNullable(transformationTraceService.getAscendantOfInstanceByModelType(keycloakModel.getName(), AsmModel.class, client))
                    .filter(o -> o instanceof EClass).map(o -> (EClass) o)
                    .filter(actorType -> AsmUtils.getExtensionAnnotationCustomValue(actorType, "actorType", "kind", false)
                            .filter(kind -> "HUMAN".equalsIgnoreCase(kind))
                            .isPresent())
                    .isPresent();

            if (humanActor) {
                clientData.setPublicClient(client.getPublicClient() != null ? client.getPublicClient() : humanDefaultSystemAccessType == AccessType.PUBLIC);
                clientData.setBearerOnly(client.getBearerOnly() != null ? client.getBearerOnly() : humanDefaultSystemAccessType == AccessType.BEARER_ONLY);
            } else {
                clientData.setPublicClient(client.getPublicClient() != null ? client.getPublicClient() : systemDefaultAccessType == AccessType.PUBLIC);
                clientData.setBearerOnly(client.getBearerOnly() != null ? client.getBearerOnly() : systemDefaultAccessType == AccessType.BEARER_ONLY);
            }

            if (client.getSecret() != null) {
                clientData.setSecret(client.getSecret());
            }
            clientData.getRedirectUris().addAll(client.getRedirectUris());
            if (corsAllowOrigin != null) {
                clientData.getWebOrigins().addAll(corsAllowOrigin);
            }
            keycloakAdminClient.createOrUpdateClient(realm.getRealm(), clientData, false);
        } else {
            clientData = MapProxy.builder(Client.class).withMap(ImmutableMap.of(
                    "redirectUris", new BasicEList(),
                    "webOrigins", new BasicEList()
            )).newInstance();

            boolean dirty = false;
            clientData.setId(existingClient.get().getId());
            clientData.setClientId(existingClient.get().getClientId());
            clientData.setName(client.getName());

            if (client.getEnabled() != null && !Objects.equals(client.getEnabled(), existingClient.get().getEnabled())) {
                dirty = true;
                clientData.setEnabled(client.getEnabled());
            }
            if (client.getDirectAccessGrantsEnabled() != null && !Objects.equals(client.getDirectAccessGrantsEnabled(), existingClient.get().getDirectAccessGrantsEnabled())) {
                dirty = true;
                clientData.setDirectAccessGrantsEnabled(client.getDirectAccessGrantsEnabled());
            }
            if (client.getPublicClient() != null && !Objects.equals(client.getPublicClient(), existingClient.get().getPublicClient())) {
                dirty = true;
                clientData.setPublicClient(client.getPublicClient());
            }
            if (client.getBearerOnly() != null && !Objects.equals(client.getBearerOnly(), existingClient.get().getBearerOnly())) {
                dirty = true;
                clientData.setBearerOnly(client.getBearerOnly());
            }
            if (!client.getRedirectUris().isEmpty() && (existingClient.get().getRedirectUris() == null || !existingClient.get().getRedirectUris().containsAll(client.getRedirectUris()))) {
                dirty = true;
                final Set<String> set = new TreeSet<>();
                set.addAll(existingClient.get().getRedirectUris());
                set.addAll(client.getRedirectUris());
                clientData.getRedirectUris().addAll(set);
            } else {
                clientData.getRedirectUris().addAll(existingClient.get().getRedirectUris());
            }
            if (corsAllowOrigin != null && !corsAllowOrigin.isEmpty() && existingClient.get().getWebOrigins() != null && !existingClient.get().getWebOrigins().containsAll(corsAllowOrigin)) {
                dirty = true;
                final Set<String> set = new TreeSet<>();
                set.addAll(existingClient.get().getWebOrigins());
                set.addAll(corsAllowOrigin);
                clientData.getWebOrigins().addAll(set);
            } else {
                clientData.getWebOrigins().addAll(existingClient.get().getWebOrigins());
            }

            if (dirty) {
                keycloakAdminClient.createOrUpdateClient(realm.getRealm(), clientData, true);
            }
        }
    }
}
