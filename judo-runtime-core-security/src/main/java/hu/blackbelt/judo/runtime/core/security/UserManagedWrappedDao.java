package hu.blackbelt.judo.runtime.core.security;

/*-
 * #%L
 * JUDO Services Composite Data Access Objects
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

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class UserManagedWrappedDao<ID> implements DAO<ID> {
    private static final String ROLLBACK_KEY = "ROLLBACK";

    private Context context;


    private DAO<ID> delegatee;

    @Setter
    private UserManager<String> userManager;

    private IdentifierProvider<ID> identifierProvider;

    private Boolean userManagerEnabled = true;

    @Builder
    public UserManagedWrappedDao(
            @NonNull DAO<ID> delegatee,
            UserManager<String> userManager,
            @NonNull Context context,
            @NonNull IdentifierProvider<ID> identifierProvider,
            Boolean userManagerEnabled) {
        this.delegatee = delegatee;
        this.userManager = userManager;
        this.context = context;
        this.identifierProvider = identifierProvider;
        this.userManagerEnabled = Optional.ofNullable(userManagerEnabled).orElse(this.userManagerEnabled);
    }

    public Payload getStaticFeatures(EClass clazz) {
        return delegatee.getStaticFeatures(clazz);
    }

    public Payload getStaticData(EAttribute attribute) {
        return delegatee.getStaticData(attribute);
    }

    @Override
    public Payload getParameterizedStaticData(EAttribute attribute, Map<String, Object> parameters) {
        return delegatee.getParameterizedStaticData(attribute, parameters);
    }

    @Override
    public Payload getDefaultsOf(EClass clazz) {
        return delegatee.getDefaultsOf(clazz);
    }

    @Override
    public Collection<Payload> getRangeOf(EReference reference, Payload payload, QueryCustomizer<ID> queryCustomizer, boolean stateful) {
        return delegatee.getRangeOf(reference, payload, queryCustomizer, stateful);
    }

    @Override
    public long countRangeOf(EReference reference, Payload payload, QueryCustomizer<ID> queryCustomizer, boolean stateful) {
        return delegatee.countRangeOf(reference, payload, queryCustomizer, stateful);
    }

    public List<Payload> getAllOf(EClass clazz) {
        return delegatee.getAllOf(clazz);
    }

    @Override
    public long countAllOf(EClass clazz) {
        return delegatee.countAllOf(clazz);
    }

    public List<Payload> search(EClass clazz, QueryCustomizer<ID> queryCustomizer) {
        return delegatee.search(clazz, queryCustomizer);
    }

    @Override
    public long count(EClass clazz, QueryCustomizer<ID> queryCustomizer) {
        return delegatee.count(clazz, queryCustomizer);
    }

    public Optional<Payload> getByIdentifier(EClass clazz, ID identifier) {
        return delegatee.getByIdentifier(clazz, identifier);
    }

    @Override
    public Optional<Payload> searchByIdentifier(EClass clazz, ID identifier, QueryCustomizer<ID> queryCustomizer) {
        return delegatee.searchByIdentifier(clazz, identifier, queryCustomizer);
    }

    @Override
    public boolean existsById(EClass clazz, ID identifier) {
        return delegatee.existsById(clazz, identifier);
    }

    @Override
    public Optional<Payload> getMetadata(EClass clazz, ID identifier) {
        return delegatee.getMetadata(clazz, identifier);
    }

    public List<Payload> getByIdentifiers(EClass clazz, Collection<ID> identifiers) {
        return delegatee.getByIdentifiers(clazz, identifiers);
    }

    @Override
    public List<Payload> searchByIdentifiers(EClass clazz, Collection<ID> identifiers, QueryCustomizer<ID> queryCustomizer) {
        return delegatee.searchByIdentifiers(clazz, identifiers, queryCustomizer);
    }

    public Payload create(EClass clazz, Payload payload, QueryCustomizer<ID> queryCustomizer) {
        checkArgument(userManager != null || !userManagerEnabled,"User manager is not started yet");
        final Payload result = delegatee.create(clazz, payload, queryCustomizer);
        if (userManager != null && !Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))) {
            userManager.getManagedActorOfPrincipal(clazz).ifPresent(actorType -> userManager.createUser(actorType, convertPrincipalToActor(clazz, result)));
        }
        return result;
    }

    @Override
    public List<Payload> createAll(EClass clazz, Iterable<Payload> payloads, QueryCustomizer<ID> queryCustomizer) {
        checkArgument(userManager != null || !userManagerEnabled,"User manager is not started yet");
        final List<Payload> results = delegatee.createAll(clazz, payloads, queryCustomizer);
        if (userManager != null
                && !Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))
                && userManager.getManagedActorOfPrincipal(clazz).isPresent()
                && results.size() > 0) {
                    userManager.getManagedActorOfPrincipal(clazz).ifPresent(actorType -> userManager.createUser(actorType, convertPrincipalToActor(clazz, results.get(0))));
        }
        return results;
    }

    public Payload update(EClass clazz, Payload payload, QueryCustomizer<ID> queryCustomizer) {
        checkArgument(userManager != null || !userManagerEnabled,"User manager is not started yet");
        if (!Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))) {
            final Optional<EClass> managedActorType = userManager != null ? userManager.getManagedActorOfPrincipal(clazz) : Optional.empty();
            final Optional<String> username;
            if (managedActorType.isPresent()) {
                Optional<Payload> loadedUser = delegatee.getByIdentifier(clazz, payload.getAs(identifierProvider.getType(), identifierProvider.getName()));
                checkArgument(loadedUser.isPresent(), "No user found to update");
                username = managedActorType.map(actorType -> userManager.getUsername(clazz, loadedUser.get()).orElse(null));
                checkState(username.isPresent(), "Unknown username of user to update");
            } else {
                username = null;
            }
            final Payload result = delegatee.update(clazz, payload, queryCustomizer);
            managedActorType.ifPresent(actorType -> {
                checkArgument(Objects.equals(userManager.getUsername(clazz, result).get(), username.get()), "Username is not changeable");
                userManager.updateUser(actorType, username.get(), convertPrincipalToActor(clazz, result));
            });
            return result;
        } else {
            return delegatee.update(clazz, payload, queryCustomizer);
        }
    }

    @Override
    public List<Payload> updateAll(EClass clazz, Iterable<Payload> payloads, QueryCustomizer<ID> queryCustomizer) {
        checkArgument(userManager != null || !userManagerEnabled,"User manager is not started yet");
        if (!Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))) {
            final Optional<EClass> managedActorType = userManager != null ? userManager.getManagedActorOfPrincipal(clazz) : Optional.empty();
            final Optional<String> username;
            if (managedActorType.isPresent()) {
                Collection<ID> identifiers = new ArrayList<>();
                payloads.forEach(payload -> identifiers.add(payload.getAs(identifierProvider.getType(), identifierProvider.getName())));
                List<Payload> loadedUsers = delegatee.getByIdentifiers(clazz, identifiers);
                checkArgument(loadedUsers.size() > 0, "No user found to update");
                username = managedActorType.map(actorType -> userManager.getUsername(clazz, loadedUsers.get(0)).orElse(null));
                checkState(username.isPresent(), "Unknown username of user to update");
            } else {
                username = null;
            }
            final List<Payload> results = delegatee.updateAll(clazz, payloads, queryCustomizer);
            managedActorType.ifPresent(actorType -> {
                if (results.size() > 0) {
                    checkArgument(Objects.equals(userManager.getUsername(clazz, results.get(0)).get(), username.get()), "Username is not changeable");
                    userManager.updateUser(actorType, username.get(), convertPrincipalToActor(clazz, results.get(0)));
                }
            });
            return results;
        } else {
            return delegatee.updateAll(clazz, payloads, queryCustomizer);
        }
    }

    public void delete(EClass clazz, ID id) {
        checkArgument(userManager != null || !userManagerEnabled, "User manager is not started yet");
        final Optional<EClass> actorType = userManager != null ? userManager.getManagedActorOfPrincipal(clazz) : Optional.empty();
        if (actorType.isPresent() && !Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))) {
            final Optional<Payload> loadedUser = delegatee.getByIdentifier(clazz, id);
            delegatee.delete(clazz, id);
            loadedUser.ifPresent(payload -> userManager.deleteUser(actorType.get(), userManager.getUsername(clazz, payload).get()));
        } else {
            delegatee.delete(clazz, id);
        }
    }

    public void deleteAll(EClass clazz, Iterable<ID> ids) {
        checkArgument(userManager != null || !userManagerEnabled, "User manager is not started yet");
        final Optional<EClass> actorType = userManager != null ? userManager.getManagedActorOfPrincipal(clazz) : Optional.empty();
        List<ID> idList = new ArrayList<>();
        ids.forEach(idList::add);
        if (actorType.isPresent() && !Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))) {
            final List<Payload> loadedUsers = delegatee.getByIdentifiers(clazz, idList);
            delegatee.deleteAll(clazz, ids);
            if (loadedUsers.size() > 0) {
                Payload loadedUser = loadedUsers.get(0);
                userManager.deleteUser(actorType.get(), userManager.getUsername(clazz, loadedUser).get());
            }
        } else {
            delegatee.deleteAll(clazz, ids);
        }
    }

    public void setReference(EReference reference, ID id, Collection<ID> referencedIds) {
        delegatee.setReference(reference, id, referencedIds);
    }

    public void unsetReference(EReference reference, ID id) {
        delegatee.unsetReference(reference, id);
    }

    public void addReferences(EReference reference, ID id, Collection<ID> referencedIds) {
        delegatee.addReferences(reference, id, referencedIds);
    }

    public void removeReferences(EReference reference, ID id, Collection<ID> referencedIds) {
        delegatee.removeReferences(reference, id, referencedIds);
    }

    public List<Payload> getAllReferencedInstancesOf(EReference reference, EClass clazz) {
        return delegatee.getAllReferencedInstancesOf(reference, clazz);
    }

    @Override
    public long countAllReferencedInstancesOf(EReference reference, EClass clazz) {
        return delegatee.countAllReferencedInstancesOf(reference, clazz);
    }

    public List<Payload> searchReferencedInstancesOf(EReference reference, EClass clazz, QueryCustomizer<ID> queryCustomizer) {
        return delegatee.searchReferencedInstancesOf(reference, clazz, queryCustomizer);
    }

    @Override
    public long countReferencedInstancesOf(EReference reference, EClass clazz, QueryCustomizer<ID> queryCustomizer) {
        return delegatee.countReferencedInstancesOf(reference, clazz, queryCustomizer);
    }

    public Payload updateReferencedInstancesOf(EClass clazz, EReference reference, Payload payload, QueryCustomizer<ID> queryCustomizer) {
        checkArgument(userManager != null || !userManagerEnabled,"User manager is not started yet");
        if (!Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))) {
            final Optional<EClass> managedActorType = userManager != null ? userManager.getManagedActorOfPrincipal(clazz) : Optional.empty();
            final Optional<String> username;
            if (managedActorType.isPresent()) {
                Optional<Payload> loadedUser = delegatee.getByIdentifier(clazz, payload.getAs(identifierProvider.getType(), identifierProvider.getName()));
                checkArgument(loadedUser.isPresent(), "No user found to update");
                username = managedActorType.map(actorType -> userManager.getUsername(clazz, loadedUser.get()).orElse(null));
                checkState(username.isPresent(), "Unknown username of user to update");
            } else {
                username = null;
            }
            final Payload result = delegatee.updateReferencedInstancesOf(clazz, reference, payload, queryCustomizer);
            managedActorType.ifPresent(actorType -> {
                checkArgument(Objects.equals(userManager.getUsername(clazz, result).get(), username.get()), "Username is not changeable");
                userManager.updateUser(actorType, username.get(), convertPrincipalToActor(clazz, result));
            });
            return result;
        } else {
            return delegatee.updateReferencedInstancesOf(clazz, reference, payload, queryCustomizer);
        }
    }

    public void deleteReferencedInstancesOf(EClass clazz, EReference reference, Payload payload) {
        checkArgument(userManager != null || !userManagerEnabled,"User manager is not started yet");
        final Optional<EClass> actorType = userManager != null ? userManager.getManagedActorOfPrincipal(clazz) : Optional.empty();
        if (actorType.isPresent() && !Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))) {
            final ID idToDelete = payload.getAs(identifierProvider.getType(), identifierProvider.getName());
            final Optional<Payload> loadedUser = delegatee.getByIdentifier(reference.getEReferenceType(), idToDelete);
            delegatee.deleteReferencedInstancesOf(clazz, reference, payload);
            loadedUser.ifPresent(p -> userManager.deleteUser(actorType.get(), userManager.getUsername(reference.getEReferenceType(), p).get()));
        } else {
            delegatee.deleteReferencedInstancesOf(clazz, reference, payload);
        }
    }

    public void setReferencesOfReferencedInstancesOf(EReference reference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        delegatee.setReferencesOfReferencedInstancesOf(reference, referenceToSet, instanceId, referencedIds);
    }

    public void unsetReferencesOfReferencedInstancesOf(EReference reference, EReference referenceToSet, ID instanceId) {
        delegatee.unsetReferencesOfReferencedInstancesOf(reference, referenceToSet, instanceId);
    }

    public void addAllReferencesOfReferencedInstancesOf(EReference reference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        delegatee.addAllReferencesOfReferencedInstancesOf(reference, referenceToSet, instanceId, referencedIds);
    }

    public void removeAllReferencesOfReferencedInstancesOf(EReference reference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        delegatee.removeAllReferencesOfReferencedInstancesOf(reference, referenceToSet, instanceId, referencedIds);
    }

    public List<Payload> getNavigationResultAt(ID id, EReference reference) {
        return delegatee.getNavigationResultAt(id, reference);
    }

    @Override
    public long countNavigationResultAt(ID id, EReference reference) {
        return delegatee.countNavigationResultAt(id, reference);
    }

    public List<Payload> searchNavigationResultAt(ID id, EReference reference, QueryCustomizer<ID> queryCustomizer) {
        return delegatee.searchNavigationResultAt(id, reference, queryCustomizer);
    }

    @Override
    public long countNavigationResultAt(ID id, EReference reference, QueryCustomizer<ID> queryCustomizer) {
        return delegatee.countNavigationResultAt(id, reference, queryCustomizer);
    }

    public Payload createNavigationInstanceAt(ID id, EReference reference, Payload payload, QueryCustomizer<ID> queryCustomizer) {
        checkArgument(userManager != null || !userManagerEnabled,"User manager is not started yet");
        final Payload result = delegatee.createNavigationInstanceAt(id, reference, payload, queryCustomizer);
        if (userManager != null && !Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))) {
            userManager.getManagedActorOfPrincipal(reference.getEReferenceType()).ifPresent(actorType -> userManager.createUser(actorType, convertPrincipalToActor(reference.getEReferenceType(), result)));
        }
        return result;
    }

    public Payload updateNavigationInstanceAt(ID id, EReference reference, Payload payload, QueryCustomizer<ID> queryCustomizer) {
        checkArgument(userManager != null || !userManagerEnabled,"User manager is not started yet");
        if (!Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))) {
            final Optional<EClass> managedActorType = userManager != null ? userManager.getManagedActorOfPrincipal(reference.getEReferenceType()) : Optional.empty();
            final Optional<String> username;
            if (managedActorType.isPresent()) {
                Optional<Payload> loadedUser = delegatee.getByIdentifier(reference.getEReferenceType(), payload.getAs(identifierProvider.getType(), identifierProvider.getName()));
                checkArgument(loadedUser.isPresent(), "No user found to update");
                username = managedActorType.map(actorType -> userManager.getUsername(reference.getEReferenceType(), loadedUser.get()).orElse(null));
                checkState(username.isPresent(), "Unknown username of user to update");
            } else {
                username = null;
            }
            final Payload result = delegatee.updateNavigationInstanceAt(id, reference, payload, queryCustomizer);
            managedActorType.ifPresent(actorType -> {
                checkArgument(Objects.equals(userManager.getUsername(reference.getEReferenceType(), result).get(), username.get()), "Username is not changeable");
                userManager.updateUser(actorType, username.get(), convertPrincipalToActor(reference.getEReferenceType(), result));
            });
            return result;
        } else {
            return delegatee.updateNavigationInstanceAt(id, reference, payload, queryCustomizer);
        }
    }

    public void deleteNavigationInstanceAt(ID id, EReference reference, Payload payload) {
        checkArgument(userManager != null || !userManagerEnabled,"User manager is not started yet");
        final Optional<EClass> actorType = userManager != null ? userManager.getManagedActorOfPrincipal(reference.getEReferenceType()) : Optional.empty();
        if (actorType.isPresent() && !Boolean.TRUE.equals(context.getAs(Boolean.class, ROLLBACK_KEY))) {
            final ID idToDelete = payload.getAs(identifierProvider.getType(), identifierProvider.getName());
            final Optional<Payload> loadedUser = delegatee.getByIdentifier(reference.getEReferenceType(), idToDelete);
            delegatee.deleteNavigationInstanceAt(id, reference, payload);
            loadedUser.ifPresent(p -> userManager.deleteUser(actorType.get(), userManager.getUsername(reference.getEReferenceType(), p).get()));
        } else {
            delegatee.deleteNavigationInstanceAt(id, reference, payload);
        }
    }

    public void setReferencesOfNavigationInstanceAt(ID id, EReference reference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        delegatee.setReferencesOfNavigationInstanceAt(id, reference, referenceToSet, instanceId, referencedIds);
    }

    public void unsetReferenceOfNavigationInstanceAt(ID id, EReference reference, EReference referenceToSet, ID instanceId) {
        delegatee.unsetReferenceOfNavigationInstanceAt(id, reference, referenceToSet, instanceId);
    }

    public void addAllReferencesOfNavigationInstanceAt(ID id, EReference reference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        delegatee.addAllReferencesOfNavigationInstanceAt(id, reference, referenceToSet, instanceId, referencedIds);
    }

    public void removeAllReferencesOfNavigationInstanceAt(ID id, EReference reference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        delegatee.removeAllReferencesOfNavigationInstanceAt(id, reference, referenceToSet, instanceId, referencedIds);
    }

    private Payload convertPrincipalToActor(final EClass principalType, final Payload principal) {
        return Payload.asPayload(userManager.getPrincipalAttributeMapping(principalType).entrySet().stream()
                .filter(e -> e.getValue() != null && principal.get(e.getValue()) != null)
                .collect(Collectors.toMap(e -> e.getKey(), e -> principal.get(e.getValue()))));
    }
}
