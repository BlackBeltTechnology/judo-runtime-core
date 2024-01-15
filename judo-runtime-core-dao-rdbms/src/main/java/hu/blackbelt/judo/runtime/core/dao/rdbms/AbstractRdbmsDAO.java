package hu.blackbelt.judo.runtime.core.dao.rdbms;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.MetricsCancelToken;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.BasicEMap;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.*;

@Slf4j(topic = "dao-rdbms")
public abstract class AbstractRdbmsDAO<ID> implements DAO<ID> {

    private static final String METRICS_DAO_QUERY = "dao-query";

    private static final String METRICS_DAO_COUNT = "dao-count";

    private final EMap<EClass, Boolean> hasStaticFeaturesMap = ECollections.asEMap(new ConcurrentHashMap<>());

    @Override
    public Payload getStaticFeatures(EClass clazz) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            final Payload result = readStaticFeatures(clazz);
            logResult(result);
            return result;
        }
    }

    @Override
    public Payload getParameterizedStaticData(EAttribute attribute, Map<String, Object> parameters) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            final Payload result = readStaticData(attribute, parameters);
            logResult(result);
            return result;
        }
    }

    @Override
    public Payload getStaticData(EAttribute attribute) {
        return getParameterizedStaticData(attribute, null);
    }

    @Override
    public Payload getDefaultsOf(EClass clazz) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            Payload result = readDefaultsOf(clazz);
            logResult(result);
            return result;
        }
    }

    @Override
    public Collection<Payload> getRangeOf(EReference reference, Payload payload, QueryCustomizer<ID> queryCustomizer, boolean stateful) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            Collection<Payload> result = readRangeOf(reference, payload, queryCustomizer, stateful);
            logResult(result);
            return result;
        }
    }

    @Override
    public long countRangeOf(EReference reference, Payload payload, QueryCustomizer<ID> queryCustomizer, boolean stateful) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_COUNT)) {
            long result = calculateNumberRangeOf(reference, payload, queryCustomizer, stateful);
            logResult(result);
            return result;
        }
    }

    @Override
    public List<Payload> getAllOf(EClass eClass) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            List<Payload> result = readAll(eClass);
            final EMap<EClass, Payload> cache = new BasicEMap<>();
            result.forEach(record -> addStaticFeaturesToPayload(record, eClass, cache));
            logResult(result);
            return result;
        }
    }

    @Override
    public long countAllOf(EClass eClass) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_COUNT)) {
            long result = countAll(eClass);
            logResult(result);
            return result;
        }
    }

    @Override
    public List<Payload> search(EClass eClass, QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            List<Payload> result = searchByFilter(eClass, queryCustomizer);
            final EMap<EClass, Payload> cache = new BasicEMap<>();
            result.forEach(record -> addStaticFeaturesToPayload(record, eClass, cache));
            logResult(result);
            return result;
        }
    }

    @Override
    public long count(EClass eClass, QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_COUNT)) {
            long result = countByFilter(eClass, queryCustomizer);
            logResult(result);
            return result;
        }
    }

    @Override
    public Optional<Payload> getByIdentifier(EClass clazz, ID identifier) {
        return searchByIdentifier(clazz, identifier, null);
    }

    @Override
    public Optional<Payload> searchByIdentifier(EClass clazz, ID identifier, QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            return readByIdentifier(clazz, identifier, queryCustomizer);
        }
    }

    @Override
    public boolean existsById(EClass clazz, ID identifier) {
        return searchByIdentifier(clazz, identifier, null).isPresent();
    }

    @Override
    public Optional<Payload> getMetadata(EClass clazz, ID identifier) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            return readMetadataByIdentifier(clazz, identifier);
        }
    }

    @Override
    public List<Payload> getByIdentifiers(EClass clazz, Collection<ID> identifiers) {
        return searchByIdentifiers(clazz, identifiers, null);
    }

    @Override
    public List<Payload> searchByIdentifiers(EClass clazz, Collection<ID> identifiers, QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            List<Payload> result = ImmutableList.copyOf(readByIdentifiers(clazz, identifiers, queryCustomizer));
            final EMap<EClass, Payload> cache = new BasicEMap<>();
            result.forEach(record -> addStaticFeaturesToPayload(record, clazz, cache));
            logResult(result);
            return result;
        }
    }

    @Override
    public Payload create(EClass eClass, Payload payload, QueryCustomizer<ID> queryCustomizer) {
        return create(eClass, payload, queryCustomizer, true);
    }

    @SneakyThrows(SQLException.class)
    protected Payload create(EClass eClass, Payload payload, QueryCustomizer<ID> queryCustomizer, boolean checkMandatoryFeatures) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            Payload result = insertPayload(eClass, payload, queryCustomizer, checkMandatoryFeatures);
            addStaticFeaturesToPayload(payload, eClass, new BasicEMap<>());
            logResult(result);
            return result;
        }
    }

    @Override
    public List<Payload> createAll(EClass eClass, Iterable<Payload> payloads, QueryCustomizer<ID> queryCustomizer) {
        List resultPayloads = new ArrayList<>();

        for (Payload payload : payloads) {
            resultPayloads.add(create(eClass, payload, queryCustomizer));
        }

        return ImmutableList.copyOf(resultPayloads);
    }

    @Override
    public Payload update(EClass eClass, Payload payload, QueryCustomizer<ID> queryCustomizer) {
        return update(eClass, payload, queryCustomizer, true);
    }

    @SneakyThrows(SQLException.class)
    protected Payload update(EClass eClass, Payload payload, QueryCustomizer<ID> queryCustomizer, boolean checkMandatoryFeatures) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            checkArgument(payload.containsKey(getIdentifierProvider().getName()), "Identifier not found on payload");
            Optional<Payload> original = readByIdentifier(eClass, (ID) payload.getAs(getIdentifierProvider().getType(),
                    getIdentifierProvider().getName()), null);
            checkState(original.isPresent(), "Could not found type: " + AsmUtils.getClassifierFQName(eClass) + " ID: " +
                    payload.get(getIdentifierProvider().getName()));

            Payload result = updatePayload(eClass, original.get(), payload, queryCustomizer, checkMandatoryFeatures);
            addStaticFeaturesToPayload(payload, eClass, new BasicEMap<>());
            logResult(result);
            return result;
        }
    }

    @Override
    public List<Payload> updateAll(EClass eClass, Iterable<Payload> payloads, QueryCustomizer<ID> queryCustomizer) {
        List resultPayloads = new ArrayList<>();

        for (Payload payload : payloads) {
            resultPayloads.add(update(eClass, payload, queryCustomizer));
        }

        return ImmutableList.copyOf(resultPayloads);
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void delete(EClass eClass, ID id) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            deletePayload(eClass, ImmutableSet.of(id));
        }
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void deleteAll(EClass eClass, Iterable<ID> ids) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            List<ID> idList = new ArrayList<>();
            ids.forEach(idList::add);
            deletePayload(eClass, idList);
        }
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void setReference(EReference eReference, ID id, Collection<ID> collection) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            setReferenceOfInstance(eReference, id, collection);
        }
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void unsetReference(EReference eReference, ID id) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            checkArgument(!eReference.isMany(), "Reference to unset must be single");
            unsetReferenceOfInstance(eReference, id);
        }
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void addReferences(EReference eReference, ID id, Collection<ID> collection) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            checkArgument(eReference.isMany(), "Reference to add must be many");

            addReferencesOfInstance(eReference, id, collection);
        }
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void removeReferences(EReference eReference, ID id, Collection<ID> collection) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            checkArgument(eReference.isMany(), "Reference to remove must be many");

            removeReferencesOfInstance(eReference, id, collection);
        }
    }

    @Override
    public List<Payload> getAllReferencedInstancesOf(EReference eReference, EClass eClass) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            checkNotNull(eReference.getEReferenceType());
            checkArgument(AsmUtils.equals(eReference.getEReferenceType(), eClass) || eReference.getEReferenceType().getEAllSuperTypes().contains(eClass));

            List<Payload> result = readAllReferences(eReference, null);
            final EMap<EClass, Payload> cache = new BasicEMap<>();
            result.forEach(record -> addStaticFeaturesToPayload(record, eClass, cache));
            logResult(result);
            return result;
        }
    }

    @Override
    public long countAllReferencedInstancesOf(EReference eReference, EClass eClass) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_COUNT)) {
            checkNotNull(eReference.getEReferenceType());
            checkArgument(AsmUtils.equals(eReference.getEReferenceType(), eClass) || eReference.getEReferenceType().getEAllSuperTypes().contains(eClass));

            long result = countAllReferences(eReference, null);
            logResult(result);
            return result;
        }
    }

    @Override
    public List<Payload> searchReferencedInstancesOf(EReference eReference, EClass eClass, QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            checkNotNull(eReference.getEReferenceType());
            checkArgument(AsmUtils.equals(eReference.getEReferenceType(), eClass) || eReference.getEReferenceType().getEAllSuperTypes().contains(eClass));

            List<Payload> result = searchReferences(eReference, null, queryCustomizer);
            final EMap<EClass, Payload> cache = new BasicEMap<>();
            result.forEach(record -> addStaticFeaturesToPayload(record, eClass, cache));
            logResult(result);
            return result;
        }
    }

    @Override
    public long countReferencedInstancesOf(EReference eReference, EClass eClass, QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_COUNT)) {
            checkNotNull(eReference.getEReferenceType());
            checkArgument(AsmUtils.equals(eReference.getEReferenceType(), eClass) || eReference.getEReferenceType().getEAllSuperTypes().contains(eClass));

            long result = countReferences(eReference, null, queryCustomizer);
            logResult(result);
            return result;
        }
    }

    @Override
    public Payload updateReferencedInstancesOf(EClass eClass, EReference eReference, Payload payload, QueryCustomizer<ID> queryCustomizer) {
        List<Payload> referencedInstances = getAllReferencedInstancesOf(eReference, eClass);
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(payload.get(identifierKey), i.get(identifierKey))), "Payload to update is not found in referenced instances");

        return update(eClass, payload, queryCustomizer);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void deleteReferencedInstancesOf(EClass eClass, EReference eReference, Payload payload) {
        List<Payload> referencedInstances = getAllReferencedInstancesOf(eReference, eClass);
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(payload.get(identifierKey), i.get(identifierKey))), "Payload to delete is not found in referenced instances");

        delete(eClass, (ID) payload.get(identifierKey));
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void setReferencesOfReferencedInstancesOf(EReference reference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        List<Payload> referencedInstances = getAllReferencedInstancesOf(reference, reference.getEReferenceType());
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(instanceId, i.get(identifierKey))), "Payload to update (set reference) is not found in referenced instances");

        setReferenceOfInstance(referenceToSet, instanceId, referencedIds);
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void unsetReferencesOfReferencedInstancesOf(EReference reference, EReference referenceToSet, ID instanceId) {
        checkArgument(!referenceToSet.isMany(), "Reference to unset must be single");
        List<Payload> referencedInstances = getAllReferencedInstancesOf(reference, reference.getEReferenceType());
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(instanceId, i.get(identifierKey))), "Payload to update (unset reference) is not found in referenced instances");

        unsetReferenceOfInstance(referenceToSet, instanceId);
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void addAllReferencesOfReferencedInstancesOf(EReference reference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        checkArgument(referenceToSet.isMany(), "Reference to add must be many");
        List<Payload> referencedInstances = getAllReferencedInstancesOf(reference, reference.getEReferenceType());
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(instanceId, i.get(identifierKey))), "Payload to update (add all references) is not found in referenced instances");

        addReferencesOfInstance(referenceToSet, instanceId, referencedIds);
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void removeAllReferencesOfReferencedInstancesOf(EReference reference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        checkArgument(referenceToSet.isMany(), "Reference to remove must be many");
        List<Payload> referencedInstances = getAllReferencedInstancesOf(reference, reference.getEReferenceType());
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(instanceId, i.get(identifierKey))), "Payload to update (remove all references) is not found in referenced instances");

        removeReferencesOfInstance(referenceToSet, instanceId, referencedIds);
    }

    @Override
    public List<Payload> getNavigationResultAt(ID id, EReference eReference) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            checkNotNull(eReference.getEReferenceType(), "Invalid reference");
            List<Payload> result = readAllReferences(eReference, Collections.singleton(id));
            final EMap<EClass, Payload> cache = new BasicEMap<>();
            result.forEach(record -> addStaticFeaturesToPayload(record, eReference.getEReferenceType(), cache));
            logResult(result);
            return result;
        }
    }

    @Override
    public long countNavigationResultAt(ID id, EReference eReference) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            checkNotNull(eReference.getEReferenceType(), "Invalid reference");
            long result = countAllReferences(eReference, Collections.singleton(id));
            logResult(result);
            return result;
        }
    }

    @Override
    public List<Payload> searchNavigationResultAt(ID id, EReference eReference, QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            checkNotNull(eReference.getEReferenceType(), "Invalid reference");
            List<Payload> result = searchReferences(eReference, Collections.singleton(id), queryCustomizer);
            final EMap<EClass, Payload> cache = new BasicEMap<>();
            result.forEach(record -> addStaticFeaturesToPayload(record, eReference.getEReferenceType(), cache));
            logResult(result);
            return result;
        }
    }

    @Override
    public long countNavigationResultAt(ID id, EReference eReference, QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_COUNT)) {
            checkNotNull(eReference.getEReferenceType(), "Invalid reference");
            long result = countReferences(eReference, Collections.singleton(id), queryCustomizer);
            logResult(result);
            return result;
        }
    }

    @Override
    @SneakyThrows(SQLException.class)
    public Payload createNavigationInstanceAt(ID id, EReference eReference, Payload payload, QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = getMetricsCollector().start(METRICS_DAO_QUERY)) {
            Payload result = insertPayloadAndAttach(eReference, id, payload, queryCustomizer);
            addStaticFeaturesToPayload(payload, eReference.getEReferenceType(), new BasicEMap<>());
            logResult(result);
            return result;
        }
    }

    @Override
    public Payload updateNavigationInstanceAt(ID id, EReference eReference, Payload payload, QueryCustomizer<ID> queryCustomizer) {
        List<Payload> referencedInstances = getNavigationResultAt(id, eReference);
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(payload.get(identifierKey), i.get(identifierKey))), "Payload to update is not found in referenced instances");

        return update(eReference.getEReferenceType(), payload, queryCustomizer);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void deleteNavigationInstanceAt(ID id, EReference eReference, Payload payload) {
        List<Payload> referencedInstances = getNavigationResultAt(id, eReference);
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(payload.get(identifierKey), i.get(identifierKey))), "Payload to delete is not found in referenced instances");

        delete(eReference.getEReferenceType(), (ID) payload.get(identifierKey));
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void setReferencesOfNavigationInstanceAt(ID id, EReference eReference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        List<Payload> referencedInstances = getNavigationResultAt(id, eReference);
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(instanceId, i.get(identifierKey))), "Payload to update (set reference) is not found in referenced instances");

        setReferenceOfInstance(referenceToSet, instanceId, referencedIds);
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void unsetReferenceOfNavigationInstanceAt(ID id, EReference eReference, EReference referenceToSet, ID instanceId) {
        checkArgument(!referenceToSet.isMany(), "Reference to unset must be single");
        List<Payload> referencedInstances = getNavigationResultAt(id, eReference);
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(instanceId, i.get(identifierKey))), "Payload to update (unset reference) is not found in referenced instances");

        unsetReferenceOfInstance(referenceToSet, instanceId);
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void addAllReferencesOfNavigationInstanceAt(ID id, EReference eReference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        checkArgument(referenceToSet.isMany(), "Reference to add must be many");
        List<Payload> referencedInstances = getNavigationResultAt(id, eReference);
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(instanceId, i.get(identifierKey))), "Payload to update (add all references) is not found in referenced instances");

        addReferencesOfInstance(referenceToSet, instanceId, referencedIds);
    }

    @Override
    @SneakyThrows(SQLException.class)
    public void removeAllReferencesOfNavigationInstanceAt(ID id, EReference eReference, EReference referenceToSet, ID instanceId, Collection<ID> referencedIds) {
        checkArgument(referenceToSet.isMany(), "Reference to remove must be many");
        List<Payload> referencedInstances = getNavigationResultAt(id, eReference);
        String identifierKey = getIdentifierProvider().getName();
        checkArgument(referencedInstances.stream().anyMatch(i -> Objects.equals(instanceId, i.get(identifierKey))), "Payload to update (remove all references) is not found in referenced instances");

        removeReferencesOfInstance(referenceToSet, instanceId, referencedIds);
    }

    private static void logResult(final Object result) {
        if (log.isTraceEnabled()) {
            log.trace("DAO result:\n{}", result);
        }
    }

    private boolean addStaticFeaturesToPayload(final Payload payload, final EClass clazz, final EMap<EClass, Payload> loadedPayloads) {
        if (Boolean.FALSE.equals(hasStaticFeaturesMap.get(clazz))) {
            // no static feature to load
            return false;
        }

        clazz.getEAllSuperTypes().stream()
                .filter(s -> AsmUtils.annotatedAsTrue(s, "transferObjectType") && !getAsmUtils().isMappedTransferObjectType(s))
                .forEach(unmappedType -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Adding static features of {}", AsmUtils.getClassifierFQName(unmappedType));
                    }
                    final Payload staticFeatures;
                    if (loadedPayloads.containsKey(unmappedType)) {
                        staticFeatures = loadedPayloads.get(unmappedType);
                    } else {
                        staticFeatures = getStaticFeatures(unmappedType);
                        loadedPayloads.put(unmappedType, staticFeatures);
                    }
                    if (!hasStaticFeaturesMap.containsKey(clazz) && !staticFeatures.isEmpty()) {
                        hasStaticFeaturesMap.put(clazz, true);
                    }
                    payload.putAll(Payload.asPayload(staticFeatures));
                });
        clazz.getEAllReferences().stream()
                .filter(r -> AsmUtils.isEmbedded(r) && !r.isTransient() && getAsmUtils().isMappedTransferObjectType(r.getEReferenceType()) && payload.get(r.getName()) != null)
                .forEach(reference -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Adding static features: {}", AsmUtils.getReferenceFQName(reference));
                    }
                    if (reference.isMany()) {
                        payload.getAsCollectionPayload(reference.getName()).forEach(containment -> {
                            final boolean added = addStaticFeaturesToPayload(containment, reference.getEReferenceType(), loadedPayloads);
                            if (added) {
                                hasStaticFeaturesMap.put(clazz, true);
                            }
                        });
                    } else {
                        final boolean added = addStaticFeaturesToPayload(payload.getAsPayload(reference.getName()), reference.getEReferenceType(), loadedPayloads);
                        if (added) {
                            hasStaticFeaturesMap.put(clazz, true);
                        }
                    }
                });
        if (!hasStaticFeaturesMap.containsKey(clazz)) {
            hasStaticFeaturesMap.put(clazz, false);
        }
        return hasStaticFeaturesMap.get(clazz);
    }

    protected abstract AsmUtils getAsmUtils();

    protected abstract IdentifierProvider<ID> getIdentifierProvider();

    protected abstract Payload readStaticFeatures(EClass clazz);

    protected abstract Payload readStaticData(EAttribute attribute, Map<String, Object> parameters);

    protected abstract List<Payload> readAll(EClass clazz);

    protected abstract long countAll(EClass clazz);

    protected abstract List<Payload> searchByFilter(EClass clazz, QueryCustomizer<ID> queryCustomizer);

    protected abstract long countByFilter(EClass clazz, QueryCustomizer<ID> queryCustomizer);

    protected abstract List<Payload> readAllReferences(EReference reference, Collection<ID> navigationSourceIdentifiers);

    protected abstract long countAllReferences(EReference reference, Collection<ID> navigationSourceIdentifiers);

    protected abstract List<Payload> searchReferences(EReference reference, Collection<ID> navigationSourceIdentifiers, QueryCustomizer<ID> queryCustomizer);

    protected abstract long countReferences(EReference reference, Collection<ID> navigationSourceIdentifiers, QueryCustomizer<ID> queryCustomizer);

    protected abstract Collection<Payload> readByIdentifiers(EClass clazz, Collection<ID> identifiers, QueryCustomizer<ID> queryCustomizer);

    protected abstract Optional<Payload> readByIdentifier(EClass clazz, ID identifier, QueryCustomizer<ID> queryCustomizer);

    protected abstract Optional<Payload> readMetadataByIdentifier(EClass clazz, ID identifier);

    protected abstract Payload insertPayload(EClass clazz, Payload payload, QueryCustomizer<ID> queryCustomizer, boolean checkMandatoryFeatures) throws SQLException;

    protected abstract Payload insertPayloadAndAttach(EReference reference, ID identifier, Payload payload, QueryCustomizer<ID> queryCustomizer) throws SQLException;

    protected abstract void deletePayload(EClass clazz, Collection<ID> ids) throws SQLException;

    protected abstract Payload updatePayload(EClass clazz, Payload original, Payload updated, QueryCustomizer<ID> queryCustomizer, boolean checkMandatoryFeatures) throws SQLException;

    protected abstract void setReferenceOfInstance(EReference mappedReference, ID id, Collection<ID> identifiersToSet) throws SQLException;

    protected abstract void unsetReferenceOfInstance(EReference mappedReference, ID id) throws SQLException;

    protected abstract void addReferencesOfInstance(EReference mappedReference, ID id, Collection<ID> identifiersToAdd) throws SQLException;

    protected abstract void removeReferencesOfInstance(EReference mappedReference, ID id, Collection<ID> identifiersToRemove) throws SQLException;

    protected abstract Payload readDefaultsOf(EClass clazz);

    protected abstract Collection<Payload> readRangeOf(EReference reference, Payload payload, QueryCustomizer<ID> queryCustomizer, boolean stateful);

    protected abstract long calculateNumberRangeOf(EReference reference, Payload payload, QueryCustomizer<ID> queryCustomizer, boolean stateful);

    protected abstract MetricsCollector getMetricsCollector();
}
