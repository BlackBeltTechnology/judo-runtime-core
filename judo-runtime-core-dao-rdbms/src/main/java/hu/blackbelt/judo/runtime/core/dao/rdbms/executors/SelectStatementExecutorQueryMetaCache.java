package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

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
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.RdbmsMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.emf.ecore.EReference;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor.*;

@Slf4j
public class SelectStatementExecutorQueryMetaCache {

    final SubSelect query;

    final Map<String, Node> sources = new HashMap<>();
    final Map<String, Node> types = new HashMap<>();
    final Map<String, Node> versions = new HashMap<>();
    final Map<String, Node> createUserIds = new HashMap<>();
    final Map<String, Node> createUsernames = new HashMap<>();
    final Map<String, Node> createTimestamps = new HashMap<>();
    final Map<String, Node> updateUsernames = new HashMap<>();
    final Map<String, Node> updateUserIds = new HashMap<>();
    final Map<String, Node> updateTimestamps = new HashMap<>();

    final Map<String, Node> metaFields = new HashMap<>();

    final Map<String, String> metaFieldNames = new HashMap<>();

    final Map<String, List<Target>> idFieldTargets = new HashMap<>();

    final Map<String, List<Target>> metaFieldTargets = new HashMap<>();

    final Map<String, FeatureTargetMapping> featureTargetMappingMap = new HashMap<>();

    final Map<List<EReference>, Target> allTargetPaths;

    final Map<Target, List<List<EReference>>> pathByTarget;

    final List<Pair<List<EReference>, List<SubSelect>>> singleEmbeddedReferences;

    final Map<Target, List<ReferencedTarget>> singleContainmentReferenceTargets = new HashMap<>();

    final Map<Target, List<ReferencedTarget>> multipleContainmentReferenceTargets = new HashMap<>();

    public SelectStatementExecutorQueryMetaCache(final SubSelect query, Map<String, Object> mask, List<EReference> referenceChain) {
        this.query = query;

        sources.putAll(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(k -> RdbmsAliasUtil.getIdColumnAlias(k).toLowerCase(), j -> j)));
        sources.put(RdbmsAliasUtil.getIdColumnAlias(query.getSelect()).toLowerCase(), query.getSelect());

        types.putAll(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(k -> RdbmsAliasUtil.getTypeColumnAlias(k).toLowerCase(), j -> j)));
        types.put(RdbmsAliasUtil.getTypeColumnAlias(query.getSelect()).toLowerCase(), query.getSelect());
        metaFieldNames.putAll(types.entrySet().stream().collect(Collectors.toMap(k -> k.getKey(), j -> ENTITY_TYPE_MAP_KEY)));

        versions.putAll(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(k -> RdbmsAliasUtil.getVersionColumnAlias(k).toLowerCase(), j -> j)));
        versions.put(RdbmsAliasUtil.getVersionColumnAlias(query.getSelect()).toLowerCase(), query.getSelect());
        metaFieldNames.putAll(versions.entrySet().stream().collect(Collectors.toMap(k -> k.getKey(), j -> ENTITY_VERSION_MAP_KEY)));

        createUsernames.putAll(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(k -> RdbmsAliasUtil.getCreateUsernameColumnAlias(k).toLowerCase(), j -> j)));
        createUsernames.put(RdbmsAliasUtil.getCreateUsernameColumnAlias(query.getSelect()).toLowerCase(), query.getSelect());
        metaFieldNames.putAll(createUsernames.entrySet().stream().collect(Collectors.toMap(k -> k.getKey(), j -> ENTITY_CREATE_USERNAME_MAP_KEY)));

        createUserIds.putAll(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(k -> RdbmsAliasUtil.getCreateUserIdColumnAlias(k).toLowerCase(), j -> j)));
        createUserIds.put(RdbmsAliasUtil.getCreateUserIdColumnAlias(query.getSelect()).toLowerCase(), query.getSelect());
        metaFieldNames.putAll(createUserIds.entrySet().stream().collect(Collectors.toMap(k -> k.getKey(), j -> ENTITY_CREATE_USER_ID_MAP_KEY)));

        createTimestamps.putAll(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(k -> RdbmsAliasUtil.getCreateTimestampColumnAlias(k).toLowerCase(), j -> j)));
        createTimestamps.put(RdbmsAliasUtil.getCreateTimestampColumnAlias(query.getSelect()).toLowerCase(), query.getSelect());
        metaFieldNames.putAll(createTimestamps.entrySet().stream().collect(Collectors.toMap(k -> k.getKey(), j -> ENTITY_CREATE_TIMESTAMP_MAP_KEY)));

        updateUsernames.putAll(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(k -> RdbmsAliasUtil.getUpdateUsernameColumnAlias(k).toLowerCase(), j -> j)));
        updateUsernames.put(RdbmsAliasUtil.getUpdateUsernameColumnAlias(query.getSelect()).toLowerCase(), query.getSelect());
        metaFieldNames.putAll(updateUsernames.entrySet().stream().collect(Collectors.toMap(k -> k.getKey(), j -> ENTITY_UPDATE_USERNAME_MAP_KEY)));

        updateUserIds.putAll(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(k -> RdbmsAliasUtil.getUpdateUserIdColumnAlias(k).toLowerCase(), j -> j)));
        updateUserIds.put(RdbmsAliasUtil.getUpdateUserIdColumnAlias(query.getSelect()).toLowerCase(), query.getSelect());
        metaFieldNames.putAll(updateUserIds.entrySet().stream().collect(Collectors.toMap(k -> k.getKey(), j -> ENTITY_UPDATE_USER_ID_MAP_KEY)));

        updateTimestamps.putAll(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(k -> RdbmsAliasUtil.getUpdateTimestampColumnAlias(k).toLowerCase(), j -> j)));
        updateTimestamps.put(RdbmsAliasUtil.getUpdateTimestampColumnAlias(query.getSelect()).toLowerCase(), query.getSelect());
        metaFieldNames.putAll(updateTimestamps.entrySet().stream().collect(Collectors.toMap(k -> k.getKey(), j -> ENTITY_UPDATE_TIMESTAMP_MAP_KEY)));

        metaFields.putAll(types);
        metaFields.putAll(versions);
        metaFields.putAll(createUsernames);
        metaFields.putAll(createUserIds);
        metaFields.putAll(createTimestamps);
        metaFields.putAll(updateUsernames);
        metaFields.putAll(updateUserIds);
        metaFields.putAll(updateTimestamps);

        sources.forEach((s, n) -> {
            idFieldTargets.put(s.toLowerCase(), query.getSelect().getTargets().stream()
                    .filter(t -> Objects.equals(t.getNode(), n))
                    .collect(Collectors.toList()));
        });

        metaFields.forEach((s, n) -> {
            metaFieldTargets.put(s.toLowerCase(), query.getSelect().getTargets().stream()
                    .filter(t -> Objects.equals(t.getNode(), n))
                    .collect(Collectors.toList()));
        });

        for (Target target : query.getSelect().getTargets()) {
            featureTargetMappingMap.putAll(query.getSelect().getFeatures().stream()
                    .flatMap(f -> f.getTargetMappings().stream().filter(
                                    tm -> Objects.equals(tm.getTarget(), target) &&
                                            tm.getTargetAttribute() != null))
                    .collect(Collectors.toMap(tm ->
                                    RdbmsAliasUtil.getTargetColumnAlias(target, RdbmsMapper.getAttributeOrFeatureName(tm.getTargetAttribute(), null)).toLowerCase(),
                            Function.identity())));
        }


        allTargetPaths = getAllTargetPaths(query.getSelect().getMainTarget());
        pathByTarget = allTargetPaths.entrySet().stream()
                .collect(Collectors.groupingBy(Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

        singleEmbeddedReferences = allTargetPaths.entrySet().stream()
                .filter(tp -> tp.getKey().stream().allMatch(r -> isEmbedded(r) && !r.isMany()))
                .map(tp -> Pair.<List<EReference>, List<SubSelect>>of(
                        ImmutableList.<EReference>builder().addAll(referenceChain).addAll(tp.getKey()).build(),
                        ImmutableList.copyOf(tp.getValue().getNode().getSubSelects().stream()
                                .filter(subSelect -> subSelect.getTransferRelation() != null && isEmbedded(subSelect.getTransferRelation()) && !subSelect.isExcluding())
                                .filter(subSelect -> mask == null || mask.containsKey(subSelect.getTransferRelation().getName())).collect(Collectors.toList()))))
                .collect(Collectors.toList());

        for (Target target : query.getSelect().getTargets()) {
            singleContainmentReferenceTargets.put(target, target.getReferencedTargets().stream()
                    .filter(rt -> isEmbedded(rt.getReference()))
                    .filter(c -> !c.getReference().isMany())
                    .filter(c -> mask == null || getMaskForTarget(c.getTarget(), mask, pathByTarget) != null).toList());

            multipleContainmentReferenceTargets.put(target, target.getReferencedTargets().stream()
                    .filter(r -> isEmbedded(r.getReference()))
                    .filter(c -> c.getReference().isMany())
                    .filter(c -> mask == null || mask.containsKey(c.getReference().getName())).toList());
         }
    }

    private Map<List<EReference>, Target> getAllTargetPaths(final Target target) {
        final Map<List<EReference>, Target> result = new HashMap<>();
        result.put(new ArrayList<>(), target);
        return addReferencesToAllTargetPaths(result, Collections.emptyList(), target.getReferencedTargets());
    }

    private Map<List<EReference>, Target> addReferencesToAllTargetPaths(final Map<List<EReference>, Target> found, final List<EReference> path, final List<ReferencedTarget> processing) {
        if (processing.isEmpty()) {
            return found;
        }

        final Map<List<EReference>, Target> newTargets = processing.stream()
                .filter(r -> !found.containsValue(r.getTarget()))
                .collect(Collectors.toMap(r -> ImmutableList.<EReference>builder().addAll(path).add(r.getReference()).build(), ReferencedTarget::getTarget));
        found.putAll(newTargets);

        newTargets.forEach((key, value) -> addReferencesToAllTargetPaths(found, key, value.getReferencedTargets()));
        return found;
    }

    private boolean isEmbedded(final EReference reference) {
        return AsmUtils.annotatedAsTrue(reference, "embedded");
    }

    private Map<String, Object> getMaskForTarget(Target target, Map<String, Object> mask, Map<Target, List<List<EReference>>> pathByTarget) {
        Map<String, Object> subMask = mask;
        if (mask != null) {
            for (List<EReference> references : pathByTarget.get(target)) {
                Map<String, Object> maskRes = null;
                for (EReference reference : references) {
                    if (maskRes == null) {
                        if (subMask != null && subMask.containsKey(reference.getName())) {
                            maskRes = (Map<String, Object>) subMask.get(reference.getName());
                        } else {
                            maskRes = null;
                        }
                    }
                }
                subMask = maskRes;
            }
        }
        return subMask;
    }


    public Optional<String> getMetaFieldName(String fieldName) {
        return Optional.ofNullable(metaFieldNames.get(fieldName.toLowerCase()));
    }

    public Map<String, Node> getSources() {
        return sources;
    }

    public Map<String, Node> getTypes() {
        return types;
    }

    public Map<String, Node> getVersions() {
        return versions;
    }

    public Map<String, Node> getCreateUserIds() {
        return createUserIds;
    }

    public Map<String, Node> getCreateUsernames() {
        return createUsernames;
    }

    public Map<String, Node> getCreateTimestamps() {
        return createTimestamps;
    }

    public Map<String, Node> getUpdateUsernames() {
        return updateUsernames;
    }

    public Map<String, Node> getUpdateUserIds() {
        return updateUserIds;
    }

    public Map<String, Node> getUpdateTimestamps() {
        return updateTimestamps;
    }

    public Optional<Node> getSources(String field) {
        return Optional.ofNullable(sources.get(field.toLowerCase()));
    }

    public Optional<Node> getTypes(String field) {
        return Optional.ofNullable(types.get(field.toLowerCase()));
    }

    public Optional<Node> getVersions(String field) {
        return Optional.ofNullable(versions.get(field.toLowerCase()));
    }

    public Optional<Node> getCreateUserIds(String field) {
        return Optional.ofNullable(createUserIds.get(field.toLowerCase()));
    }

    public Optional<Node> getCreateUsernames(String field) {
        return Optional.ofNullable(createUsernames.get(field.toLowerCase()));
    }

    public Optional<Node> getCreateTimestamps(String field) {
        return Optional.ofNullable(createTimestamps.get(field.toLowerCase()));
    }

    public Optional<Node> getUpdateUsernames(String field) {
        return Optional.ofNullable(updateUsernames.get(field.toLowerCase()));
    }

    public Optional<Node> getUpdateUserIds(String field) {
        return Optional.ofNullable(updateUserIds.get(field.toLowerCase()));
    }

    public Optional<Node> getUpdateTimestamps(String field) {
        return Optional.ofNullable(updateTimestamps.get(field.toLowerCase()));
    }

    public Optional<Node> getMetaField(String field) {
        return Optional.ofNullable(metaFields.get(field.toLowerCase()));
    }


    public Optional<List<Target>> getIdFieldTargets(String field) {
        return Optional.ofNullable(idFieldTargets.get(field.toLowerCase()));
    }


    public Optional<List<Target>> getMetaFieldTargets(String field) {
        return Optional.ofNullable(metaFieldTargets.get(field.toLowerCase()));
    }


    public Optional<FeatureTargetMapping> getFeatureTargetMapping(String field) {
        return Optional.ofNullable(featureTargetMappingMap.get(field.toLowerCase()));
    }

    public List<Pair<List<EReference>, List<SubSelect>>> getSingleEmbeddedReferences() {
        return singleEmbeddedReferences;
    }

    public List<ReferencedTarget> getSingleContainmentReferenceTargets(Target target) {
        return singleContainmentReferenceTargets.get(target);
    }

    public List<ReferencedTarget> getMultipleContainmentReferenceTargets(Target target) {
        return multipleContainmentReferenceTargets.get(target);
    }

}
