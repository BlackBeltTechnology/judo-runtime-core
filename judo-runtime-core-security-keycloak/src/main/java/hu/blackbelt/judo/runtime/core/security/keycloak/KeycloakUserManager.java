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

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.keycloak.*;
import hu.blackbelt.judo.meta.keycloak.runtime.KeycloakModel;
import hu.blackbelt.judo.runtime.core.security.PasswordPolicy;
import hu.blackbelt.judo.runtime.core.security.UserManager;
import hu.blackbelt.judo.tatami.core.TransformationTrace;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakConnector.logError;
import static java.util.function.Function.identity;

@Slf4j
public class KeycloakUserManager implements UserManager<String> {

    public static final String KEYCLOAK_ID = "id";
    public static final String KEYCLOAK_USERNAME_CLAIM = "username";
    public static final String KEYCLOAK_EMAIL_CLAIM = "email";
    public static final String KEYCLOAK_CREDENTIALS_CLAIM = "credentials";
    public static final String KEYCLOAK_REQUIRED_ACTIONS_CLAIM = "requiredActions";
    public static final String KEYCLOAK_PASSWORD_TYPE = "password";
    private static final String EMAIL_CLAIM_TYPE = "EMAIL";
    private static final String USERNAME_CLAIM_TYPE = "USERNAME";


    private AsmModel asmModel;
    private PasswordPolicy<String> defaultPasswordPolicy;
    private TransformationTraceService transformationTraceService;
    private TransformationTrace transformationTrace;
    private KeycloakAdminClient keycloakAdminClient;
    private Boolean enabled = true;
    private Boolean updateExistingUsers = false;
    private String requiredActions = null;
    private Boolean asyncServiceCall = true;
    private Integer retryMaxAttempts = 1000;
    private Boolean retryExponentialBackoff = true;
    private Long retryWaitDuration = 1000L;

    private Collection<String> requiredActionsOnUserCreate;

    final EMap<EClass, Optional<Realm>> realmsOfActors = ECollections.asEMap(new ConcurrentHashMap<>());
    final EMap<EAttribute, Collection<String>> attributeBindings = ECollections.asEMap(new ConcurrentHashMap<>());

    private AsmUtils asmUtils;
    private EMap<EClass, EClass> managedActorPrincipals;
    private EMap<EClass, Map<String, String>> attributeMappingOfPrincipal = ECollections.asEMap(new ConcurrentHashMap<>());

    private RetryRegistry retryRegistry;

    @Setter
    private boolean identityManagerReady;

    @Builder
    public KeycloakUserManager(
            @NonNull AsmModel asmModel,
            @NonNull PasswordPolicy<String> defaultPasswordPolicy,
            @NonNull TransformationTraceService transformationTraceService,
            @NonNull TransformationTrace transformationTrace,
            @NonNull KeycloakAdminClient keycloakAdminClient,
            Boolean enabled,
            Boolean updateExistingUsers,
            String requiredActions,
            Boolean asyncServiceCall,
            Integer retryMaxAttempts,
            Boolean retryExponentialBackoff,
            Long retryWaitDuration
    ) {
        this.asmModel = asmModel;
        this.defaultPasswordPolicy = defaultPasswordPolicy;
        this.transformationTraceService = transformationTraceService;
        this.transformationTrace = transformationTrace;
        this.keycloakAdminClient = keycloakAdminClient;
        this.enabled = Optional.ofNullable(enabled).orElse(this.enabled);
        this.updateExistingUsers = Optional.ofNullable(updateExistingUsers).orElse(this.updateExistingUsers);
        this.requiredActions = Optional.ofNullable(requiredActions).orElse(this.requiredActions);
        this.asyncServiceCall = Optional.ofNullable(asyncServiceCall).orElse(this.asyncServiceCall);
        this.retryMaxAttempts = Optional.ofNullable(retryMaxAttempts).orElse(this.retryMaxAttempts);
        this.retryExponentialBackoff = Optional.ofNullable(retryExponentialBackoff).orElse(this.retryExponentialBackoff);
        this.retryWaitDuration = Optional.ofNullable(retryWaitDuration).orElse(this.retryWaitDuration);
        if (requiredActions != null) {
            requiredActionsOnUserCreate = Arrays.asList(requiredActions.split(",")).stream().map(a -> a.trim()).collect(Collectors.toList());
        }

        if (!enabled) {
            log.warn("User manager is not enabled");
        }

        asmUtils = new AsmUtils(asmModel.getResourceSet());
        managedActorPrincipals = ECollections.asEMap(asmUtils.getAllActorTypes().stream()
                .filter(actorType -> asmUtils.isMappedTransferObjectType(actorType) &&
                        AsmUtils.getExtensionAnnotationCustomValue(actorType, "actorType", "managed", false)
                                .map(v -> Boolean.parseBoolean(v)).orElse(false))
                .filter(actorType -> actorType.getEAllOperations().stream()
                        .anyMatch(op -> Objects.equals(AsmUtils.getBehaviour(op).orElse(null), AsmUtils.OperationBehaviour.GET_PRINCIPAL)))
                .collect(Collectors.toList()).stream()
                .collect(Collectors.toMap(actorType -> actorType.getEAllOperations().stream()
                                .filter(op -> Objects.equals(AsmUtils.getBehaviour(op).orElse(null), AsmUtils.OperationBehaviour.GET_PRINCIPAL))
                                .map(op -> (EClass) op.getEType())
                                .findAny().orElse(null),
                        identity()
                )));

        attributeMappingOfPrincipal.clear();

        retryRegistry = RetryUtil.createRetryRegistry(retryMaxAttempts, retryWaitDuration, retryExponentialBackoff);
    }

    @Override
    public Optional<Map<String, Object>> getUser(final EClass actor, final String username) {
        if (enabled) {
            return getRealmOfActor(actor)
                    .map(realmName -> keycloakAdminClient.getUserOfRealm(realmName, username)
                            .map(user -> convertFromKeycloakPayload(actor, user)))
                    .orElse(null);
        } else {
            log.info("User manager is not enabled, returning no user for username '{}'", username);
            return Optional.empty();
        }
    }

    @Override
    public List<Map<String, Object>> getAllUsers(final EClass actor) {
        if (enabled) {
            return getRealmOfActor(actor)
                    .map(realmName -> keycloakAdminClient.getUsersOfRealm(realmName).stream()
                            .map(user -> convertFromKeycloakPayload(actor, user))
                            .collect(Collectors.toList()))
                    .orElse(null);
        } else {
            log.info("User manager is not enabled, returning no users");
            return Collections.emptyList();
        }
    }

    @Override
    public void createUser(final EClass actor, final Map<String, Object> user) {
        final String username = getUsernameByActorType(actor, user).orElseThrow(() -> new IllegalStateException("Username is empty"));
        if (enabled) {
            getRealmOfActor(actor)
                    .ifPresent(realmName -> {
                        Retry retry = retryRegistry.retry("createUserCall Realm: " + realmName + " User:  " + username);
                        Runnable task = Retry.decorateRunnable(retry,
                                createUserCall(realmName, username, user));

                        RetryUtil.registerLogEventHandlers(retry);

                        if (asyncServiceCall) {
                            CompletableFuture.runAsync(task).whenComplete((v, e) -> {
                                if (e != null) {
                                    log.error("Could not create user: {} of realm: {}", username, realmName, e);
                                }
                            });
                        } else {
                            task.run();
                        }
                    });
        } else {
            log.info("User manager is not enabled, user is not created: '{}'", username);
        }
    }

    private Runnable createUserCall(String realmName, String username, final Map<String, Object> user) {
        return () -> {
            checkState(identityManagerReady);
            final Optional<Map<String, Object>> existingUser;
            if (updateExistingUsers) {
                existingUser = keycloakAdminClient.getUserOfRealm(realmName, username);
            } else {
                existingUser = Optional.empty(); // do not get existing user, just try to create it
            }
            if (!existingUser.isPresent()) {
                createUserInternal(username, user, realmName);
            } else {
                updateUserInternal(username, user, realmName, (String) existingUser.get().get(KEYCLOAK_ID));
            }
        };
    }



    @Override
    public void updateUser(final EClass actor, final String username, final Map<String, Object> user) {
        if (enabled) {
            getRealmOfActor(actor)
                    .ifPresent(realmName -> {
                        Retry retry = retryRegistry.retry("updateUserCall Realm: " + realmName + " User: " + username);
                        Runnable task = Retry.decorateRunnable(retry,
                                updateUserCall(realmName, username, user));

                        RetryUtil.registerLogEventHandlers(retry);

                        if (asyncServiceCall) {
                            CompletableFuture.runAsync(task).whenComplete((v, e) -> {
                                if (e != null) {
                                    log.error("Could not update user: {} of realm: {}", username, realmName, e);
                                }
                            });
                        } else {
                            task.run();
                        }
                    });
        } else {
            log.info("User manager is not enabled, user is not updated: '{}'", username);
        }
    }

    private Runnable updateUserCall(final String realmName, final String username, final Map<String, Object> user) {
        return () -> {
            checkState(identityManagerReady);
            final Optional<Map<String, Object>> existingUser = keycloakAdminClient.getUserOfRealm(realmName, username);
            if (!existingUser.isPresent()) {
                createUserInternal(username, user, realmName);
            } else {
                updateUserInternal(username, user, realmName, (String) existingUser.get().get(KEYCLOAK_ID));
            }
        };
    }


    private void createUserInternal(final String username, final Map<String, Object> user, final String realmName) {
        if (log.isDebugEnabled()) {
            log.debug("Creating Keycloak user: {} in realm '{}' ...", username, realmName);
        }
        final Optional<String> defaultPassword = defaultPasswordPolicy.apply(user);

        HashMap<String, Object> payload = new HashMap<>(user);
        payload.put("enabled", true);
        payload.put("username", username);

        if (defaultPassword.isPresent()) {
            HashMap<String, Object> userCredential = new HashMap<>();
            userCredential.put("type", KEYCLOAK_PASSWORD_TYPE);
            userCredential.put("temporary", false);
            userCredential.put("value", defaultPassword.get());

            payload.put(KEYCLOAK_CREDENTIALS_CLAIM, Collections.singletonList(userCredential));
        }

        if (!requiredActionsOnUserCreate.isEmpty()) {
            payload.put(KEYCLOAK_REQUIRED_ACTIONS_CLAIM, requiredActionsOnUserCreate);
        }

        Supplier<Response> task = keycloakAdminClient.createUserRestCall(realmName, payload);

        Consumer<Response> handler = (response) -> {
            if (response.getStatus() < 200 || response.getStatus() >= 300) {
                //logError(response);
                throw new IllegalStateException("Failed to create/update realm - Realm: " + realmName + " User: " + username);
            }
            log.info("Created Keycloak user: {}", username);
        };
        handler.accept(task.get());
    }

    private void updateUserInternal(final String username, final Map<String, Object> user, final String realmName, final String id) {
        if (log.isDebugEnabled()) {
            log.debug("Updating Keycloak user: {} in realm '{}'...", username, realmName);
        }

        Supplier<Response> task = keycloakAdminClient.updateUserRestCall(realmName, id, user);

        Consumer<Response> handler = (response) -> {
            if (response.getStatus() < 200 || response.getStatus() >= 300) {
                // logError(response);
                throw new IllegalStateException("Failed to create/update realm - Realm: " + realmName + " User: " + username);
            }
            log.info("Updated Keycloak user: {}", username);
        };
        handler.accept(task.get());
    }

    @Override
    public void deleteUser(final EClass actor, final String username) {
        if (enabled) {
            getRealmOfActor(actor)
                    .ifPresent(realmName -> {
                        Retry retry = retryRegistry.retry("deleteUserCall Realm: " + realmName + " User: " + username);
                        Runnable task = Retry.decorateRunnable(retry,
                                deleteUserCall(realmName, username));

                        RetryUtil.registerLogEventHandlers(retry);

                        if (asyncServiceCall) {
                            CompletableFuture.runAsync(task).whenComplete((v, e) -> {
                                if (e != null) {
                                    log.error("Could not create user: {} of realm: {}", username, realmName, e);
                                }
                            });
                        } else {
                            task.run();
                        }
                   });
        } else {
            log.info("User manager is not enabled, user is not deleted: '{}'", username);
        }
    }

    private Runnable deleteUserCall(String realmName, final String username) {
        return () -> {
            checkState(identityManagerReady);
            keycloakAdminClient.getUserOfRealm(realmName, username)
                    .ifPresent(existingUser -> {

                        if (log.isDebugEnabled()) {
                            log.debug("Deleting Keycloak user: {} from realm '{}' ...", username, realmName);
                        }

                        Consumer<Response> handler = (response) -> {
                            if (response.getStatus() < 200 || response.getStatus() >= 300) {
                                logError(response);
                                throw new IllegalStateException("Failed to create/update realm - Realm: " + realmName + " User: " + username);
                            }
                            log.info("Deleted Keycloak user: {}", username);
                        };

                        handler.accept(keycloakAdminClient.deleteUserRestCall(realmName, (String) existingUser.get("id")).get());
                    });
        };

    }

    @Override
    public Map<String, String> getPrincipalAttributeMapping(final EClass principalType) {
        if (attributeMappingOfPrincipal.containsKey(principalType)) {
            return Collections.unmodifiableMap(attributeMappingOfPrincipal.get(principalType));
        } else if (managedActorPrincipals.containsKey(principalType)) {
            final EClass actorType = managedActorPrincipals.get(principalType);

            final Map<String, String> mapping = transformationTraceService.getDescendantOfInstanceByModelType(asmModel.getName(), KeycloakModel.class, actorType).stream()
                    .filter(e -> e instanceof Client).map(e -> (Client) e)
                    .flatMap(client -> client.getAttributeBindings().stream())
                    .collect(Collectors.toMap(attributeBinding -> attributeBinding.getAttributeName(), attributeBinding -> {
                        final EAttribute actorClaim = (EAttribute) transformationTraceService.getAscendantOfInstanceByModelType(asmModel.getName(), AsmModel.class, attributeBinding);
                        checkArgument(actorClaim != null, "Claim not found for keycloak attribute binding: " + attributeBinding.getAttributeName());
                        final Optional<String> targetName = principalType.getEAllAttributes().stream()
                                .filter(transferAttribute -> AsmUtils.equals(
                                        asmUtils.getMappedAttribute(transferAttribute).orElse(null),
                                        asmUtils.getMappedAttribute(actorClaim).orElse(null)))
                                .map(transferAttribute -> transferAttribute.getName())
                                .findAny();
                        if (!targetName.isPresent()) {
                            log.error("No mapping found for actor claim: {}, mapping: {}", AsmUtils.getAttributeFQName(actorClaim), asmUtils.getMappedAttribute(actorClaim).map(a -> AsmUtils.getAttributeFQName(a)).orElse(null));
                            principalType.getEAllAttributes().forEach(transferAttribute -> {
                                log.info("  - transfer attribute: {}", AsmUtils.getAttributeFQName(transferAttribute));
                                log.info("  - mapped attribute of transferAttribute: {}", asmUtils.getMappedAttribute(transferAttribute).map(a -> AsmUtils.getAttributeFQName(a)).orElse(null));
                            });
                        }
                        return targetName.orElse(attributeBinding.getAttributeName());
                    }));
            attributeMappingOfPrincipal.put(principalType, mapping);
            return Collections.unmodifiableMap(mapping);
        } else {
            throw new UnsupportedOperationException("Invalid principal type");
        }
    }

    @Override
    public Optional<EClass> getManagedActorOfPrincipal(final EClass principalType) {
        return Optional.ofNullable(managedActorPrincipals.get(principalType));
    }

    private Optional<String> getUsernameByActorType(EClass mappedActorType, Map<String, Object> userDataByPrincipal) {
        final String username;
        if (mappedActorType.getEAllAttributes().stream().anyMatch(a -> AsmUtils.getExtensionAnnotationValue(a, "claim", false).filter(c -> USERNAME_CLAIM_TYPE.equals(c)).isPresent())) {
            username = (String) userDataByPrincipal.get(KEYCLOAK_USERNAME_CLAIM);
        } else if (mappedActorType.getEAllAttributes().stream().anyMatch(a -> AsmUtils.getExtensionAnnotationValue(a, "claim", false).filter(c -> EMAIL_CLAIM_TYPE.equals(c)).isPresent())) {
            username = (String) userDataByPrincipal.get(KEYCLOAK_EMAIL_CLAIM);
        } else {
            throw new IllegalStateException("No USERNAME nor EMAIL claim defined");
        }

        return Optional.ofNullable(username);
    }

    private Optional<String> getUsernameFromPrincipalDataByActorType(EClass principalType, EClass mappedActorType, Map<String, Object> userDataByPrincipal) {
        final String username;
        final Optional<EAttribute> usernameClaim = mappedActorType.getEAllAttributes().stream()
                .filter(a -> AsmUtils.getExtensionAnnotationValue(a, "claim", false).filter(c -> USERNAME_CLAIM_TYPE.equals(c)).isPresent())
                .findAny();
        final Optional<EAttribute> emailClaim = mappedActorType.getEAllAttributes().stream()
                .filter(a -> AsmUtils.getExtensionAnnotationValue(a, "claim", false).filter(c -> EMAIL_CLAIM_TYPE.equals(c)).isPresent())
                .findAny();
        final Optional<EAttribute> usernameAttribute = usernameClaim
                .map(c -> principalType.getEAllAttributes().stream()
                        .filter(a -> AsmUtils.equals(asmUtils.getMappedAttribute(a).orElse(null), asmUtils.getMappedAttribute(c).orElse(null)))
                        .findAny()
                        .orElse(null));
        final Optional<EAttribute> emailAttribute = emailClaim
                .map(c -> principalType.getEAllAttributes().stream()
                        .filter(a -> AsmUtils.equals(asmUtils.getMappedAttribute(a).orElse(null), asmUtils.getMappedAttribute(c).orElse(null)))
                        .findAny()
                        .orElse(null));

        if (usernameAttribute.isPresent()) {
            username = (String) userDataByPrincipal.get(usernameAttribute.get().getName());
        } else if (emailAttribute.isPresent()) {
            username = (String) userDataByPrincipal.get(emailAttribute.get().getName());
        } else {
            throw new IllegalStateException("No USERNAME nor EMAIL claim/mapped attribute is defined");
        }

        return Optional.ofNullable(username);
    }

    @Override
    public Optional<String> getUsername(EClass principalType, Map<String, Object> user) {
        if (managedActorPrincipals.containsKey(principalType)) {
            return getUsernameFromPrincipalDataByActorType(
                    principalType,
                    getManagedActorOfPrincipal(principalType).orElseThrow(() -> new IllegalStateException("No actor type found for principal")),
                    user);
        } else {
            return Optional.empty();
        }
    }

    private EMap<EAttribute, Collection<String>> getAttributeBindings(final EClass actor) {
        final EMap<EAttribute, Collection<String>> result = ECollections.asEMap(new ConcurrentHashMap<>());

        actor.getEAllAttributes().stream()
                .filter(a -> !a.isDerived())
                .forEach(attribute -> {
                    if (!attributeBindings.containsKey(attribute)) {
                        attributeBindings.put(attribute, transformationTraceService.getDescendantOfInstanceByModelType(asmModel.getName(), KeycloakModel.class, attribute).stream()
                                .filter(o -> o instanceof AttributeBinding).map(o -> ((AttributeBinding) o).getAttributeName())
                                .collect(Collectors.toList()));
                    }
                    if (!result.containsKey(attribute)) {
                        result.put(attribute, new TreeSet<>());
                    }
                    attributeBindings.get(attribute).forEach(claim -> result.get(attribute).add(claim));
                });

        return result;
    }

    private Map<String, Object> convertFromKeycloakPayload(final EClass actor, final Map<String, Object> user) {
        final EMap<EAttribute, Collection<String>> attributeBindings = getAttributeBindings(actor);
        final Map<String, Object> data = new TreeMap<>();

        attributeBindings.forEach(e -> {
            final EAttribute attribute = e.getKey();
            e.getValue().forEach(claim -> {
                final Object oldEntry = data.get(attribute.getName());
                final Object newEntry = user.get(claim);
                boolean overwrite = true;
                if (data.containsKey(attribute.getName()) && !Objects.equals(oldEntry, newEntry)) {
                    log.warn("Claim data conflicts: {}", AsmUtils.getAttributeFQName(attribute));
                    if (KEYCLOAK_USERNAME_CLAIM.equalsIgnoreCase(attribute.getName())) {
                        // do not overwrite any data by username
                        log.info("Username has been changed and not returned in payload");
                        overwrite = false;
                    }
                }
                if (overwrite) {
                    data.put(attribute.getName(), user.get(claim));
                }
            });
        });

        return data;
    }

    private Optional<Client> getClientOfActor(final EClass actor) {
        return AsmUtils.getExtensionAnnotationByName(actor, "actorType", false)
                .filter(a -> a.getDetails().get("managed") != null && Boolean.parseBoolean(a.getDetails().get("managed")))
                .map(a -> transformationTraceService.getDescendantOfInstanceByModelType(asmModel.getName(), KeycloakModel.class, actor).stream()
                        .filter(o -> o instanceof Client).map(o -> (Client) o)
                        .findAny()
                        .orElse(null));
    }

    private Optional<String> getRealmOfActor(final EClass actor) {
        if (!realmsOfActors.containsKey(actor)) {
            final Optional<Realm> realm = getClientOfActor(actor).map(client -> (Realm) client.eContainer());
            realmsOfActors.put(actor, realm);
        }

        return realmsOfActors.get(actor).map(r -> r.getRealm());
    }
}
