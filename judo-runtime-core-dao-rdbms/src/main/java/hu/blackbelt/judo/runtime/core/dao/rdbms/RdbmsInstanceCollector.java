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

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbms.support.RdbmsModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsRules.Rule;
import hu.blackbelt.judo.meta.rdbmsRules.Rules;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceGraph;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceReference;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static hu.blackbelt.judo.meta.rdbms.support.RdbmsModelResourceSupport.rdbmsModelResourceSupportBuilder;


@Slf4j
public class RdbmsInstanceCollector<ID> implements InstanceCollector<ID> {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final AsmUtils asmUtils;
    private final RdbmsResolver rdbmsResolver;
    private final RdbmsModel rdbmsModel;
    private final Coercer coercer;
    private final IdentifierProvider<ID> identifierProvider;
    private RdbmsParameterMapper<ID> rdbmsParameterMapper;

    private final AtomicReference<RdbmsModelResourceSupport> rdbmsSupport = new AtomicReference<>(null);

    private static final String IDS = "ids";

    private static final String TABLE_ALIAS_FORMAT = "_t{0,number,00}";
    private final AtomicInteger nextAliasIndex = new AtomicInteger(0);

    private final AtomicBoolean selectsCreated = new AtomicBoolean(false);
    private final EMap<EClass, RdbmsSelect> selectsByEntityType = ECollections.asEMap(new ConcurrentHashMap<>());

    @Builder
    private RdbmsInstanceCollector(
            @NonNull NamedParameterJdbcTemplate jdbcTemplate,
            @NonNull AsmModel asmModel,
            @NonNull RdbmsResolver rdbmsResolver,
            @NonNull RdbmsModel rdbmsModel,
            @NonNull Coercer coercer,
            @NonNull IdentifierProvider<ID> identifierProvider,
            @NonNull RdbmsParameterMapper<ID> rdbmsParameterMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
        this.rdbmsResolver = rdbmsResolver;
        this.rdbmsModel = rdbmsModel;
        this.coercer = coercer;
        this.identifierProvider = identifierProvider;
        this.rdbmsParameterMapper = rdbmsParameterMapper;
    }

    public RdbmsModelResourceSupport getRdbmsSupport() {
        if (rdbmsSupport.get() == null) {
            rdbmsSupport.set(rdbmsModelResourceSupportBuilder().resourceSet(rdbmsModel.getResourceSet()).build());
        }
        return rdbmsSupport.get();
    }

    public void createSelects() {
        synchronized (selectsCreated) {
            if (selectsCreated.compareAndSet(false, true)) {
                asmUtils.all(EClass.class)
                        .filter(e -> AsmUtils.isEntityType(e))
                        .forEach(entityType -> {
                            final RdbmsSelect select = RdbmsSelect.builder()
                                    .alias(MessageFormat.format(TABLE_ALIAS_FORMAT, nextAliasIndex.getAndIncrement()))
                                    .entityType(entityType)
                                    .tableName(rdbmsResolver.rdbmsTable(entityType).getSqlName())
                                    .build();

                            getContainmentsWithReferences(0, entityType, select, null);
                            selectsByEntityType.put(entityType, select);
                        });
            }
        }
    }

    @Override
    public Map<ID, InstanceGraph<ID>> collectGraph(final EClass entityType, final Collection<ID> ids) {
        createSelects();

        return collectInstances(selectsByEntityType.get(entityType), ids, rdbmsParameterMapper);
    }

    @Override
    public InstanceGraph<ID> collectGraph(final EClass entityType, final ID id) {
        Map<ID, InstanceGraph<ID>> graphs = collectGraph(entityType, ImmutableSet.of(id));
        checkArgument(graphs.containsKey(id), "Graph could not find " +
                getClassifierFQName(entityType) + " with ID " + id);
        return graphs.get(id);
    }


    private EMap<EList<EReference>, Map<ID, InstanceGraph<ID>>> processResults(final RdbmsSelect select, final List<Map<String, Object>> result, final Map<ID, InstanceGraph<ID>> baseGraphs, final Optional<String> parentIdName, final EList<EReference> referenceChain, final ReferenceType referenceType) {
        final EMap<EList<EReference>, Map<ID, InstanceGraph<ID>>> containmentIds = new BasicEMap<>();
        containmentIds.put(referenceChain, new HashMap<>());

        if (!result.isEmpty()) {
            log.trace("Processing results of base: {}, reference type: {}", select.getTableName(), referenceType);
        }

        result.forEach(record -> {
            log.trace("  - record: {}", record);
            final Optional<ID> parentId = parentIdName.map(name -> coercer.coerce(record.get(name), identifierProvider.getType()));
            final ID id = coercer.coerce(record.get(select.getAlias() + "_ID"), identifierProvider.getType());

            log.trace("    - ID: {}, parent ID: {}", id, parentId);

            final InstanceGraph<ID> graphOfRecord = InstanceGraph.<ID>builder().id(id).build();
            final Map<ID, InstanceGraph<ID>> graphs = new ConcurrentHashMap<>();
            graphs.put(id, graphOfRecord);

            if (parentId.isPresent()) {
                containmentIds.get(referenceChain).put(id, graphOfRecord);
                final InstanceReference<ID> instanceReference = InstanceReference.<ID>builder()
                        .reference(referenceChain.get(referenceChain.size() - 1))
                        .referencedElement(graphOfRecord)
                        .build();

                final InstanceGraph<ID> container = baseGraphs.get(parentId.get());
                if (container != null) {
                    switch (referenceType) {
                        case CONTAINMENT:
                            container.getContainments().add(instanceReference);
                            break;
                        case REFERENCE:
                            container.getReferences().add(instanceReference);
                            break;
                        case BACK_REFERENCE:
                            container.getBackReferences().add(instanceReference);
                            break;
                        default:
                            throw new IllegalStateException("Invalid reference type");
                    }
                } else {
                    throw new IllegalStateException("Container graph not found");
                }
            } else {
                baseGraphs.put(id, graphOfRecord);
            }

            select.getAllJoins().stream().forEach(join -> {
                if (referenceType != ReferenceType.CONTAINMENT) {
                    log.trace("Reference type {} is not supported yet for JOINed elements.", referenceType);
                    return;
                }

                final EList<EReference> path = new BasicEList<>();
                path.addAll(referenceChain);
                path.addAll(join.getAllReferences());

                log.trace("    - joined table: {} AS {}, partner: {}", new Object[]{join.getTableName(), join.getAlias(), join.getPartner().getAlias()});

                final Object joinedIdObject = record.get(join.getAlias() + "_ID");
                final ID joinedId = joinedIdObject != null ? coercer.coerce(joinedIdObject, identifierProvider.getType()) : null;

                final Object joinedPartnerIdObject = record.get(join.getPartner().getAlias() + "_ID");
                final ID joinedPartnerId = joinedPartnerIdObject != null ? coercer.coerce(joinedPartnerIdObject, identifierProvider.getType()) : null;

                log.trace("      - joined ID: {} (partner ID: {}), reference name: {}, type: {}", new Object[]{joinedId, joinedPartnerId, join.getReference() != null ? AsmUtils.getReferenceFQName(join.getReference()) : "-", join.getReferenceType()});

                if (joinedId != null && join.getReference() != null) {
                    final InstanceGraph<ID> joinedGraph = InstanceGraph.<ID>builder().id(joinedId).build();
                    graphs.put(joinedId, joinedGraph);

                    final InstanceReference<ID> instanceReference = InstanceReference.<ID>builder()
                            .reference(join.getReference())
                            .referencedElement(joinedGraph)
                            .build();

                    if (join.getReferenceType() == ReferenceType.REFERENCE) {
                        graphs.get(joinedPartnerId).getReferences().add(instanceReference);
                        log.trace("        - added as reference");
                    } else if (join.getReferenceType() == ReferenceType.BACK_REFERENCE) {
                        graphs.get(joinedPartnerId).getBackReferences().add(instanceReference);
                        log.trace("        - added as back reference");
                    } else if (join.getReferenceType() == ReferenceType.CONTAINMENT) {
                        if (!containmentIds.containsKey(path)) {
                            containmentIds.put(path, new ConcurrentHashMap<>());
                        }
                        containmentIds.get(path).put(joinedId, joinedGraph);

                        graphs.get(joinedPartnerId).getContainments().add(instanceReference);
                        log.trace("        - added as containment");
                    }
                }
            });
        });

        return containmentIds;
    }

    private Map<ID, InstanceGraph<ID>> collectInstances(final RdbmsSelect select, final Collection<ID> ids, final RdbmsParameterMapper<ID> parameterMapper) {
        final Map<ID, InstanceGraph<ID>> graphs = new HashMap<>();

        final String sql = select.toSql();
        log.debug("SQL:\n{}", sql);

        final EMap<EList<EReference>, Map<ID, InstanceGraph<ID>>> selectContainments;
        if (ids != null && !ids.isEmpty()) {
            final List<Map<String, Object>> result = jdbcTemplate.queryForList(sql, Collections.singletonMap(IDS, ids.stream().map(id -> coercer.coerce(id, parameterMapper.getIdClassName())).collect(Collectors.toList())));

            selectContainments = processResults(select, result, graphs, Optional.empty(), ECollections.emptyEList(), ReferenceType.CONTAINMENT);
        } else {
            selectContainments = ECollections.emptyEMap();
        }

        select.getSubSelects().forEach(subSelect -> {
            if (ids != null && !ids.isEmpty()) {
                collectSubSelectInstances(subSelect, graphs, ECollections.emptyEList(), parameterMapper);
            }
        });

        select.getAllJoins().stream()
                .filter(join -> join.isContainment())
                .forEach(join ->
                        join.getSubSelects().forEach(subSelect -> {
                            final Map<ID, InstanceGraph<ID>> joinedGraphs = selectContainments.get(join.getAllReferences());
                            if (joinedGraphs != null && !joinedGraphs.isEmpty()) {
                                collectSubSelectInstances(subSelect, joinedGraphs, join.getAllReferences(), parameterMapper);
                            }
                        }));

        return graphs;
    }

    private void collectSubSelectInstances(final RdbmsSubSelect subSelect, final Map<ID, InstanceGraph<ID>> graphs, final EList<EReference> prevReferenceChain, final RdbmsParameterMapper<ID> parameterMapper) {
        final EList<EReference> referenceChain = new BasicEList<>();
        referenceChain.addAll(prevReferenceChain);
        referenceChain.add(subSelect.getReference());

        if (log.isTraceEnabled()) {
            log.trace("Collecting instances of {} based on {}", referenceChain.stream().map(r -> AsmUtils.getReferenceFQName(r)).collect(Collectors.toList()), graphs.keySet());
        }

        final String subSelectSql = subSelect.toSql();

        log.debug("SQL:\n{}", subSelectSql);
        log.debug("  - parent IDs: {}", graphs.keySet());

        final boolean loopDetected = referenceChain.stream().filter(r -> AsmUtils.equals(r, subSelect.getReference())).count() > 1;
        if (loopDetected) {
            log.trace("Loop detected: {} in {}", subSelect.getReference().getName(), referenceChain.stream().map(r -> r.getName()).collect(Collectors.toList()));
        }

        final EMap<EList<EReference>, Map<ID, InstanceGraph<ID>>> subSelectContainments;
        if (!graphs.keySet().isEmpty()) {
            final List<Map<String, Object>> subQueryResults = jdbcTemplate.queryForList(subSelectSql, Collections.singletonMap(IDS, graphs.keySet().stream().map(id -> coercer.coerce(id, parameterMapper.getIdClassName())).collect(Collectors.toList())));

            final String baseAlias = subSelect.getBase().getAlias();
            subSelectContainments = processResults(subSelect.getBase().getSelect(), subQueryResults, graphs, Optional.of("_P" + baseAlias + "_ID"), referenceChain, subSelect.getReferenceType());
        } else {
            subSelectContainments = ECollections.emptyEMap();
        }

        if (subSelect.getReferenceType() == ReferenceType.CONTAINMENT) {
            if (log.isTraceEnabled()) {
                log.trace("Containments: {}", subSelectContainments.values().stream().flatMap(e -> e.keySet().stream()).collect(Collectors.toList()));
            }

            subSelect.getBase().getSelect().getSubSelects().stream()
                    .forEach(subSubSelect -> {
                        final Map<ID, InstanceGraph<ID>> subGraphs = subSelectContainments.entrySet().stream()
                                .filter(e -> !e.getValue().isEmpty())
                                .filter(e -> EcoreUtil.equals(e.getKey(), referenceChain))
                                .flatMap(e -> e.getValue().entrySet().stream())
                                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
                        if (subGraphs != null && !subGraphs.isEmpty()) {
                            collectSubSelectInstances(subSubSelect, subGraphs != null ? subGraphs : Collections.emptyMap(), referenceChain, parameterMapper);
                        }
                    });

            subSelect.getBase().getSelect().getAllJoins().stream()
                    .filter(join -> join.isContainment())
                    .forEach(join ->
                            join.getSubSelects().stream()
                                    .forEach(subSubSelect -> {
                                        final EList<EReference> nextReferenceChain = new BasicEList<>();
                                        nextReferenceChain.addAll(referenceChain);
                                        nextReferenceChain.addAll(join.getAllReferences());
                                        final Map<ID, InstanceGraph<ID>> joinedGraphs = subSelectContainments.entrySet().stream()
                                                .filter(e -> !e.getValue().isEmpty())
                                                .filter(e -> EcoreUtil.equals(e.getKey(), nextReferenceChain))
                                                .flatMap(e -> e.getValue().entrySet().stream())
                                                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
                                        if (joinedGraphs != null && !joinedGraphs.isEmpty()) {
                                            collectSubSelectInstances(subSubSelect, joinedGraphs != null ? joinedGraphs : Collections.emptyMap(), ECollections.asEList(Stream.concat(referenceChain.stream(), join.getAllReferences().stream()).collect(Collectors.toList())), parameterMapper);
                                        }
                                    }));
        }
    }

    private void getContainmentsWithReferences(final int level, final EClass entityType, final Source source, final EReference containmentOfEntityType) {
        final EMap<EClass, Source> sources = new BasicEMap<>();

        if (log.isTraceEnabled()) {
            log.trace(pad(level) + "- entity type: {}", getClassifierFQName(entityType));
        }

        // add all containments
        entityType.getEAllReferences().stream()
                .filter(reference -> reference.isContainment() /*|| AsmUtils.isCascadeDelete(reference) */)
                .filter(containment -> !containment.isDerived())
                .forEach(containment -> {
                    log.trace(pad(level) + "  - containment: {}", containment.getName());

                    if (!AsmUtils.equals(entityType, containment.getEContainingClass())) {
                        if (!sources.containsKey(containment.getEContainingClass())) {
                            final RdbmsJoin join = RdbmsJoin.builder()
                                    .alias(MessageFormat.format(TABLE_ALIAS_FORMAT, nextAliasIndex.getAndIncrement()))
                                    .entityType(containment.getEContainingClass())
                                    .tableName(rdbmsResolver.rdbmsTable(containment.getEContainingClass()).getSqlName())
                                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                                    .partner(source)
                                    .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                                    .referenceType(ReferenceType.ANCESTOR)
                                    .build();
                            sources.put(containment.getEContainingClass(), join);
                            source.getJoins().add(join);
                        }
                    } else {
                        sources.put(entityType, source);
                    }

                    final Joinable joinable = createJoin(level, containment, ReferenceType.CONTAINMENT, sources.get(containment.getEContainingClass()), false);
                    if (joinable instanceof RdbmsJoin) {
                        source.getJoins().add((RdbmsJoin) joinable);

                        getContainmentsWithReferences(level + 1, containment.getEReferenceType(), (RdbmsJoin) joinable, containment);
                    } else if (joinable instanceof RdbmsSubSelect) {
                        source.getSubSelects().add((RdbmsSubSelect) joinable);
                    }
                });

        // add all references
        entityType.getEAllReferences().stream()
                .filter(reference -> !reference.isDerived() &&
                        !reference.isContainer() && // NOTE - containers are ignored because they are processed by opposite (containment) references
                        !reference.isContainment())
                .forEach(reference -> {
                    log.trace(pad(level) + "  - reference to entity type: {}", reference.getName());

                    if (!AsmUtils.equals(entityType, reference.getEContainingClass())) {
                        if (!sources.containsKey(reference.getEContainingClass())) {
                            final RdbmsJoin join = RdbmsJoin.builder()
                                    .alias(MessageFormat.format(TABLE_ALIAS_FORMAT, nextAliasIndex.getAndIncrement()))
                                    .entityType(reference.getEContainingClass())
                                    .tableName(rdbmsResolver.rdbmsTable(reference.getEContainingClass()).getSqlName())
                                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                                    .partner(source)
                                    .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                                    .referenceType(ReferenceType.ANCESTOR)
                                    .build();
                            sources.put(reference.getEContainingClass(), join);
                            source.getJoins().add(join);
                        }
                    } else {
                        sources.put(entityType, source);
                    }

                    final Joinable joinable = createJoin(level, reference, ReferenceType.REFERENCE, sources.get(reference.getEContainingClass()), false);
                    if (joinable instanceof RdbmsJoin) {
                        source.getJoins().add((RdbmsJoin) joinable);
                    } else if (joinable instanceof RdbmsSubSelect) {
                        source.getSubSelects().add((RdbmsSubSelect) joinable);
                    }
                });

        // TODO - test if changed with reference
        // add all back references
        asmUtils.all(EReference.class)
                .filter(reference -> !reference.isDerived() &&
                        !reference.isContainer() && // NOTE - containers are ignored because they are processed by opposite (containment) references
                        (!reference.isContainment() || source instanceof RdbmsSelect) && // NOTE - if reference is a containment but source is RdbmsSelect a back reference should be checked before operations
                        (AsmUtils.equals(reference.getEReferenceType(), entityType) || entityType.getEAllSuperTypes().contains(reference.getEReferenceType())))
                .forEach(opposite -> {
                    log.trace(pad(level) + "  - opposite reference from entity type: {}.{}", getClassifierFQName(opposite.getEContainingClass()), opposite.getName());

                    final Source oppositeSource;
                    if (!AsmUtils.equals(opposite.getEReferenceType(), entityType)) {
                        if (!sources.containsKey(opposite.getEReferenceType())) {
                            final RdbmsJoin join = RdbmsJoin.builder()
                                    .alias(MessageFormat.format(TABLE_ALIAS_FORMAT, nextAliasIndex.getAndIncrement()))
                                    .entityType(opposite.getEReferenceType())
                                    .tableName(rdbmsResolver.rdbmsTable(opposite.getEReferenceType()).getSqlName())
                                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                                    .partner(source)
                                    .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                                    .referenceType(ReferenceType.ANCESTOR)
                                    .build();
                            sources.put(opposite.getEReferenceType(), join);
                            source.getJoins().add(join);
                        }
                        oppositeSource = sources.get(opposite.getEReferenceType());
                    } else {
                        oppositeSource = source;
                    }

                    final Joinable joinable = createJoin(level, opposite, ReferenceType.BACK_REFERENCE, oppositeSource, true);
                    if (joinable instanceof RdbmsJoin) {
                        source.getJoins().add((RdbmsJoin) joinable);
                    } else if (joinable instanceof RdbmsSubSelect) {
                        source.getSubSelects().add((RdbmsSubSelect) joinable);
                    }
                });
    }

    private EList<EReference> getReferenceList(final Source source) {
        if (source instanceof RdbmsSelect) {
            return new BasicEList<>();
        } else if (source instanceof RdbmsJoin) {
            final EList<EReference> list = getReferenceList(((RdbmsJoin) source).getPartner());
            list.add(((RdbmsJoin) source).getReference());
            return list;
        } else {
            throw new IllegalStateException("Invalid source");
        }
    }

    private Joinable createJoin(final int level, final EReference reference, final ReferenceType referenceType, final Source source, final boolean inverse) {
        // get RDBMS rule of a given reference
        final Rule rule = getRdbmsSupport().all()
                .filter(Rules.class::isInstance)
                .map(Rules.class::cast)
                .findFirst().get().getRuleFromReference(reference);

        final EReference opposite = reference.getEOpposite();

        // get RBDMS rule of the opposite (partner) reference
        final Optional<Rule> oppositeRule = Optional.ofNullable(opposite).map(o -> getRdbmsSupport().all()
                .filter(Rules.class::isInstance)
                .map(Rules.class::cast)
                .findFirst().get().getRuleFromReference(o));

        final RdbmsJoin.RdbmsJoinBuilder joinBuilder = RdbmsJoin.builder()
                .alias(MessageFormat.format(TABLE_ALIAS_FORMAT, nextAliasIndex.getAndIncrement()))
                .reference(reference)
                .entityType(inverse ? reference.getEContainingClass() : reference.getEReferenceType())
                .tableName(rdbmsResolver.rdbmsTable(inverse ? reference.getEContainingClass() : reference.getEReferenceType()).getSqlName())
                .partner(source)
                .referenceType(/* AsmUtils.isCascadeDelete(reference) ? ReferenceType.CONTAINMENT : */referenceType);

        final RdbmsSubSelect.RdbmsSubSelectBuilder subSelectBuilder = RdbmsSubSelect.builder()
                .reference(reference)
                .base(new RdbmsSelectReference(inverse ? reference.getEContainingClass() : reference.getEReferenceType()))
                .entityType(inverse ? reference.getEContainingClass() : reference.getEReferenceType())
                .partner(new RdbmsSelectReference(source.getEntityType()))
                .referenceType(/* AsmUtils.isCascadeDelete(reference) ? ReferenceType.CONTAINMENT : */referenceType);

        boolean createJoin = false;
        boolean createJoinTable = false;
        boolean createSubSelect = false;

        final boolean circularDependencyFound = getReferenceList(source).contains(reference);

        // TODO - add to configuration parameter how times a single containment is translated to JOIN (instead of subqueries)
        //final boolean circularDependencyFound = getReferenceList(source).stream().filter(r -> AsmUtils.equals(r, reference)).count() > 3;

        if (rule.isForeignKey() && !inverse || rule.isInverseForeignKey() && inverse) { // reference is owned by source class, target class has reference to the ID with different name
            final boolean join = !circularDependencyFound && (!inverse && !reference.isMany() || inverse && opposite != null && !opposite.isMany());
            log.trace(pad(level) + "    '{}' is foreign key -> {}", AsmUtils.getReferenceFQName(reference), join ? "JOIN" : "SUBQUERY");

            if (join) {
                joinBuilder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(rdbmsResolver.rdbmsField(reference).getSqlName());
                createJoin = true;
            } else {
                subSelectBuilder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(rdbmsResolver.rdbmsField(reference).getSqlName());
                createSubSelect = true;
            }
        } else if (rule.isInverseForeignKey() && !inverse || rule.isForeignKey() && inverse) {  // reference is owned by target class, source class has reference to the ID with different name
            final boolean join = !circularDependencyFound && (!inverse && !reference.isMany() || inverse && opposite != null && !opposite.isMany());
            log.trace(pad(level) + "    '{}' is inverse foreign key -> {}", AsmUtils.getReferenceFQName(reference), join ? "JOIN" : "SUBQUERY");

            if (join) {
                joinBuilder.columnName(rdbmsResolver.rdbmsField(reference).getSqlName()).partnerColumnName(StatementExecutor.ID_COLUMN_NAME);
                createJoin = true;
            } else {
                subSelectBuilder.columnName(rdbmsResolver.rdbmsField(reference).getSqlName()).partnerColumnName(StatementExecutor.ID_COLUMN_NAME);
                createSubSelect = true;
            }
        } else if (rule.isJoinTable()) { // JOIN table
            subSelectBuilder
                    .columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                    .junctionTableName(rdbmsResolver.rdbmsJunctionTable(reference).getSqlName())
                    .junctionColumnName(!inverse ? rdbmsResolver.rdbmsJunctionField(reference).getSqlName() : rdbmsResolver.rdbmsJunctionOppositeField(reference).getSqlName())
                    .junctionReverseColumnName(!inverse ? rdbmsResolver.rdbmsJunctionOppositeField(reference).getSqlName() : rdbmsResolver.rdbmsJunctionField(reference).getSqlName());
            createJoinTable = true;
        } else if (oppositeRule.isPresent()) { // RBDMS can use opposite (partner) reference to store relationship
            if (oppositeRule.get().isForeignKey() && !inverse || oppositeRule.get().isInverseForeignKey() && inverse) { // reference is owned by source class, target class has reference to the ID with different name (defined by opposite reference)
                final boolean join = (!reference.isMany() && !inverse || !opposite.isMany() && inverse) && !circularDependencyFound;
                log.trace(pad(level) + "    opposite of '{}' ({}) is foreign key -> {}", new Object[]{AsmUtils.getReferenceFQName(reference), AsmUtils.getReferenceFQName(opposite), join ? "JOIN" : "SUBQUERY"});

                if (join) {
                    joinBuilder.columnName(rdbmsResolver.rdbmsField(opposite).getSqlName()).partnerColumnName(StatementExecutor.ID_COLUMN_NAME);
                    createJoin = true;
                } else {
                    subSelectBuilder.columnName(rdbmsResolver.rdbmsField(opposite).getSqlName()).partnerColumnName(StatementExecutor.ID_COLUMN_NAME);
                    createSubSelect = true;
                }
            } else if (oppositeRule.get().isInverseForeignKey() && !inverse || oppositeRule.get().isForeignKey() && inverse) {  // reference is owned by target class, source class has reference to the ID with different name (defined by opposite reference)
                final boolean join = (!reference.isMany() && !inverse || !opposite.isMany() && inverse) && !circularDependencyFound;
                log.trace(pad(level) + "    opposite of '{}' ({}) is inverse foreign key -> {}", new Object[]{AsmUtils.getReferenceFQName(reference), AsmUtils.getReferenceFQName(opposite), join ? "JOIN" : "SUBQUERY"});

                if (join) {
                    joinBuilder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(rdbmsResolver.rdbmsField(opposite).getSqlName());
                    createJoin = true;
                } else {
                    subSelectBuilder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(rdbmsResolver.rdbmsField(opposite).getSqlName());
                    createSubSelect = true;
                }
            } else if (oppositeRule.get().isJoinTable()) { // JOIN table
                subSelectBuilder
                        .columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                        .junctionTableName(rdbmsResolver.rdbmsJunctionTable(reference).getSqlName())
                        .junctionColumnName(!inverse ? rdbmsResolver.rdbmsJunctionOppositeField(reference).getSqlName() : rdbmsResolver.rdbmsJunctionField(reference).getSqlName())
                        .junctionReverseColumnName(!inverse ? rdbmsResolver.rdbmsJunctionField(reference).getSqlName() : rdbmsResolver.rdbmsJunctionOppositeField(reference).getSqlName());
                createJoinTable = true;
            } else {
                throw new IllegalStateException("Invalid opposite reference");
            }
        } else {
            throw new IllegalStateException("Invalid reference");
        }

        if (createJoin) {
            return joinBuilder.build();
        } else if (createSubSelect) {
            return subSelectBuilder.build();
        } else if (createJoinTable) {
            return subSelectBuilder.build();
        } else {
            throw new IllegalStateException("Invalid JOIN");
        }
    }

    private interface Source {
        String getAlias();

        List<RdbmsJoin> getJoins();

        List<RdbmsSubSelect> getSubSelects();

        EClass getEntityType();

        String getTableName();
    }

    private interface BaseSource extends Source {
        String toSql(final RdbmsSubSelect subSelect);

        RdbmsSelect getSelect();

        EClass getEntityType();
    }

    @lombok.Getter
    @lombok.Builder
    @lombok.EqualsAndHashCode
    private static class RdbmsSelect implements BaseSource {

        @NonNull
        private String tableName;

        @NonNull
        private String alias;

        private final List<RdbmsJoin> joins = new ArrayList<>();

        private final List<RdbmsSubSelect> subSelects = new ArrayList<>();

        @NonNull
        private EClass entityType;

        public List<RdbmsJoin> getAllJoins() {
            return Stream.concat(joins.stream(), joins.stream().flatMap(j -> j.getAllJoins().stream())).collect(Collectors.toList());
        }

        public String toSql(final RdbmsSubSelect subSelect) {
            final String baseAlias = subSelect.getBase().getAlias();

            final String joinCondition;
            if (subSelect.getJunctionTableName() != null) {
                joinCondition = "EXISTS (SELECT 1 FROM " +  subSelect.getJunctionTableName() +
                        " WHERE _P" + baseAlias + "." + subSelect.getPartnerColumnName() + " = " + subSelect.getJunctionTableName() + "." + subSelect.getJunctionReverseColumnName() +
                        " AND " + subSelect.getJunctionTableName() + "." + subSelect.getJunctionColumnName() + " = " + subSelect.getBase().getAlias() + "." + subSelect.getColumnName() + ")";
            } else {
                joinCondition = "_P" + baseAlias + "." + subSelect.getPartnerColumnName() + " = " + subSelect.getBase().getAlias() + "." + subSelect.getColumnName();
            }

            return "SELECT " + alias + "." + StatementExecutor.ID_COLUMN_NAME + " AS " + alias + "_ID, " +
                    "_P" + baseAlias + "." + StatementExecutor.ID_COLUMN_NAME + " AS " + "_P" + baseAlias + "_ID" +
                    (getAllJoins().isEmpty() ? "" : ", " + getAllJoins().stream().map(j -> j.getAlias() + "." + StatementExecutor.ID_COLUMN_NAME + " AS " + j.getAlias() + "_ID").collect(Collectors.joining(", "))) +
                    "\nFROM " + tableName + " AS " + alias +
                    "\nJOIN " + subSelect.getPartner().getTableName() + " AS " + "_P" + baseAlias + " ON (" + joinCondition + ")" +
                    (getAllJoins().isEmpty() ? "" : getAllJoins().stream().map(j -> j.toSql()).collect(Collectors.joining())) +
                    "\nWHERE " + "_P" + baseAlias + "." + StatementExecutor.ID_COLUMN_NAME + " IN (:" + IDS + ")";
        }

        public String toSql() {
            return "SELECT " + alias + "." + StatementExecutor.ID_COLUMN_NAME + " AS " + alias + "_ID" +
                    (getAllJoins().isEmpty() ? "" : ", " + getAllJoins().stream().map(j -> j.getAlias() + "." + StatementExecutor.ID_COLUMN_NAME + " AS " + j.getAlias() + "_ID").collect(Collectors.joining(", "))) +
                    "\nFROM " + tableName + " AS " + alias +
                    (getAllJoins().isEmpty() ? "" : getAllJoins().stream().map(j -> j.toSql()).collect(Collectors.joining())) +
                    "\nWHERE " + alias + "." + StatementExecutor.ID_COLUMN_NAME + " IN (:" + IDS + ")";
        }

        @Override
        public String toString() {
            return toSql();
        }

        @Override
        public RdbmsSelect getSelect() {
            return this;
        }
    }

    private interface Joinable {
    }

    @lombok.Getter
    @lombok.Builder
    @lombok.EqualsAndHashCode
    private static class RdbmsJoin implements Source, Joinable {
        @NonNull
        private String tableName;
        @NonNull
        private String columnName;

        @NonNull
        private Source partner;
        @NonNull
        private String partnerColumnName;

        @NonNull
        private String alias;

        private EReference reference;

        @NonNull
        private ReferenceType referenceType;

        private final List<RdbmsJoin> joins = new ArrayList<>();

        private final List<RdbmsSubSelect> subSelects = new ArrayList<>();

        @NonNull
        private EClass entityType;

        public List<RdbmsJoin> getAllJoins() {
            return Stream.concat(joins.stream(), joins.stream().flatMap(j -> j.getAllJoins().stream())).collect(Collectors.toList());
        }

        public String toSql() {
            return "\nLEFT OUTER JOIN " + tableName + " AS " + alias + " ON (" + partner.getAlias() + "." + partnerColumnName + " = " + alias + "." + columnName + ") ";
        }

        public EList<EReference> getAllReferences() {
            final EList<EReference> allReferences = new BasicEList<>();
            if (partner instanceof RdbmsJoin) {
                allReferences.addAll(((RdbmsJoin) partner).getAllReferences());
            }
            allReferences.add(reference);
            return allReferences;
        }

        public boolean isContainment() {
            return referenceType == ReferenceType.CONTAINMENT && (partner instanceof RdbmsJoin) ? ((RdbmsJoin) partner).isContainment() : true;
        }

        @Override
        public String toString() {
            return toSql();
        }
    }

    @lombok.Data
    @lombok.EqualsAndHashCode
    private class RdbmsSelectReference implements BaseSource {

        @NonNull
        private EClass entityType;

        @Override
        public String getAlias() {
            return selectsByEntityType.get(entityType).getAlias();
        }

        @Override
        public String toSql(final RdbmsSubSelect subSelect) {
            return selectsByEntityType.get(entityType).toSql(subSelect);
        }

        @Override
        public RdbmsSelect getSelect() {
            return selectsByEntityType.get(entityType);
        }

        @Override
        public List<RdbmsJoin> getJoins() {
            return getSelect().getJoins();
        }

        @Override
        public List<RdbmsSubSelect> getSubSelects() {
            return getSelect().getSubSelects();
        }

        @Override
        public String getTableName() {
            return getSelect().getTableName();
        }
    }

    @lombok.Getter
    @lombok.Builder
    @lombok.EqualsAndHashCode
    private static class RdbmsSubSelect implements Joinable {

        @NonNull
        private BaseSource base;

        @NonNull
        private String columnName;

        @NonNull
        private Source partner;

        @NonNull
        private String partnerColumnName;

        private EReference reference;

        private String junctionTableName;
        private String junctionColumnName;
        private String junctionReverseColumnName;

        @NonNull
        private ReferenceType referenceType;

        @NonNull
        private EClass entityType;

        public String toSql() {
            return base.toSql(this);
        }

        @Override
        public String toString() {
            return toSql();
        }
    }

    private enum ReferenceType {
        CONTAINMENT,
        REFERENCE,
        BACK_REFERENCE,
        ANCESTOR
    }

    private static String pad(int level) {
        return StringUtils.leftPad("", level * 4, " ");
    }
}
