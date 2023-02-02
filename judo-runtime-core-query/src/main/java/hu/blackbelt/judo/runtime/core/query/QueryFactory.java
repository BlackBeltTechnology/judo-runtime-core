package hu.blackbelt.judo.runtime.core.query;

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

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.DataExpression;
import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.ReferenceExpression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmMeasureProvider;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilder;
import hu.blackbelt.judo.meta.measure.support.MeasureModelResourceSupport;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.runtime.QueryUtils;
import hu.blackbelt.judo.meta.query.support.QueryModelResourceSupport;
import hu.blackbelt.judo.runtime.core.expression.EntityTypeExpressions;
import hu.blackbelt.judo.runtime.core.expression.MappedTransferObjectTypeBindings;
import hu.blackbelt.judo.runtime.core.expression.TransferObjectTypeBindingsCollector;
import hu.blackbelt.judo.runtime.core.expression.UnmappedTransferObjectTypeBindings;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.emf.common.notify.Notifier;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.util.builder.EAttributeBuilder;
import org.eclipse.emf.ecore.util.builder.EClassBuilder;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static hu.blackbelt.judo.meta.query.runtime.QueryUtils.getNextSubSelectAlias;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

/**
 * Logical query factory for ASM mapped transfer object types.
 */
@Slf4j
public class QueryFactory {

    private final ResourceSet asmResourceSet;

    private final TransferObjectTypeBindingsCollector transferObjectTypeBindingsCollector;

    @Getter
    private final AtomicInteger nextSourceIndex = new AtomicInteger(0);

    @Getter
    private final AtomicInteger nextTargetIndex = new AtomicInteger(0);

    private final AsmModelAdapter modelAdapter;
    private final AsmUtils asmUtils;

    private EMap<EClass, Select> transferObjectQueries = ECollections.asEMap(new ConcurrentHashMap<>()); // key: ASM mapped transfer object type
    private EMap<EReference, SubSelect> navigationQueries = ECollections.asEMap(new ConcurrentHashMap<>()); // key: ASM exposed graph
    private EMap<EAttribute, SubSelect> dataQueries = ECollections.asEMap(new ConcurrentHashMap<>()); // key: ASM exposed graph

    public static final String TABLE_ALIAS_FORMAT = "t{0,number,00}";

    private FeatureFactory featureFactory;
    private JoinFactory joinFactory;

    @Getter
    private final EMap<EReference, CustomJoinDefinition> customJoinDefinitions;

    private final EList<EReference> orderedTransferRelations = new UniqueEList<>();

    @Getter
    private final QueryModelResourceSupport queryModelResourceSupport;

    public static final String EXPOSED_ALIAS = "__exposed";
    public static final String DATA_ALIAS = "__data";

    public QueryFactory(final ResourceSet asmResourceSet, final ResourceSet expressionResourceSet, final Coercer coercer) {
        this(asmResourceSet, MeasureModelResourceSupport.createMeasureResourceSet(), expressionResourceSet, coercer, ECollections.emptyEMap());
    }

    @Builder
    public QueryFactory(
            @NonNull final ResourceSet asmResourceSet,
            @NonNull final ResourceSet measureResourceSet,
            @NonNull final ResourceSet expressionResourceSet,
            @NonNull final Coercer coercer,
            final EMap<EReference, CustomJoinDefinition> customJoinDefinitions) {

        this.asmResourceSet = asmResourceSet;
        this.customJoinDefinitions = customJoinDefinitions == null ?  ECollections.asEMap(new ConcurrentHashMap<>()) : customJoinDefinitions;
        queryModelResourceSupport = QueryModelResourceSupport.queryModelResourceSupportBuilder()
                .uri(URI.createURI("query:in-memory"))
                .build();

        asmUtils = new AsmUtils(asmResourceSet);

        transferObjectTypeBindingsCollector = new TransferObjectTypeBindingsCollector(asmResourceSet, expressionResourceSet);

        final AsmMeasureProvider measureProvider = new AsmMeasureProvider(measureResourceSet);

        modelAdapter = new AsmModelAdapter(asmResourceSet, measureResourceSet);
        joinFactory = new JoinFactory(modelAdapter) {
            @Override
            public Feature expressionToFeature(final Expression expression, final Context context, final FeatureTargetMapping targetMapping) {
                return featureFactory.convert(expression, context, targetMapping);
            }
        };
        featureFactory = new FeatureFactory(joinFactory, modelAdapter, coercer, measureProvider);
        joinFactory.setFeatureFactory(featureFactory);

        createQueries();
        createNavigations();

        if (!queryModelResourceSupport.isValid()) {
            log.error("Invalid query model: {}", queryModelResourceSupport.getDiagnosticsAsString());
            throw new IllegalStateException("Invalid query model");
        }
    }

    public AsmModelAdapter getModelAdapter() {
        return modelAdapter;
    }

    public EMap<EClass, EntityTypeExpressions> getEntityTypeExpressionsMap() {
        return transferObjectTypeBindingsCollector.getEntityTypeExpressionsMap();
    }

    /**
     * Get logical query of a given transfer object type.
     * Logical query contains all information required to create SQL SELECT statement on ASM and no SQL table and column names and navigation (ID, foreign key name) is resolved.
     *
     * @param transferObjectType mapped transfer object type
     * @return logical query
     */
    public Optional<Select> getQuery(final EClass transferObjectType) {
        if (!asmUtils.isMappedTransferObjectType(transferObjectType)) {
            log.error("Argument is not mapped transfer object type: {}", AsmUtils.getClassifierFQName(transferObjectType));
        }

        return Optional.ofNullable(transferObjectQueries.get(transferObjectType));
    }

    // TODO: cache exposed graphs (subselects)
    public Optional<SubSelect> getNavigation(final EReference navigation) {
        return Optional.ofNullable(navigationQueries.get(navigation));
    }

    // TODO: cache static data (subselects)
    public Optional<SubSelect> getDataQuery(final EAttribute attribute) {
        return Optional.ofNullable(dataQueries.get(attribute));
    }

    public boolean isOrdered(EReference transferObjectRelation) {
        return orderedTransferRelations.contains(transferObjectRelation);
    }

    /**
     * Create logical queries for transfer object types:
     * - create logical queries for all mapped transfer object types,
     * - add references (as JOINs or SUBSELECTs)
     */
    private void createQueries() {
        // create logical query templates (main target only!)
        getAsmElement(EClass.class)
                .filter(c -> asmUtils.getMappedEntityType(c).isPresent())
                .forEach(mappedTransferObjectType -> {
                    final Optional<MappedTransferObjectTypeBindings> node = transferObjectTypeBindingsCollector.getTransferObjectGraph(mappedTransferObjectType);

                    // mapped transfer object types that have no expression tree are ignored
                    if (node.isPresent()) {
                        if (log.isTraceEnabled()) {
                            log.trace("Creating logical query skeleton for transfer object type: {}", AsmUtils.getClassifierFQName(mappedTransferObjectType));
                        }

                        // create logical query base
                        final Select select = newSelectBuilder()
                                .withFrom(node.get().getEntityType())
                                .withAlias(MessageFormat.format(TABLE_ALIAS_FORMAT, nextSourceIndex.incrementAndGet()))
                                .withMainTarget(newTargetBuilder()
                                        .withType(mappedTransferObjectType)
                                        .withIndex(nextTargetIndex.incrementAndGet())
                                        .build())
                                .build();
                        select.getTargets().add(select.getMainTarget());
                        select.getMainTarget().setContainerWithIdFeature(select, true);

                        queryModelResourceSupport.addContent(select);

                        transferObjectQueries.put(mappedTransferObjectType, select);
                    } else {
                        log.error("No root node of expression tree found for mapped transfer object type: {}, query not created", AsmUtils.getClassifierFQName(mappedTransferObjectType));
                    }
                });

        // create logical query (without references)
        getAsmElement(EClass.class)
                .filter(c -> asmUtils.getMappedEntityType(c).isPresent())
                .forEach(mappedTransferObjectType -> {
                    final Optional<MappedTransferObjectTypeBindings> node = transferObjectTypeBindingsCollector.getTransferObjectGraph(mappedTransferObjectType);

                    if (node.isPresent()) {
                        if (log.isTraceEnabled()) {
                            log.trace("Setup logical query for transfer object type: {}", AsmUtils.getClassifierFQName(mappedTransferObjectType));
                        }

                        final Select select = transferObjectQueries.get(mappedTransferObjectType);
                        final Context context = Context.builder()
                                .queryModelResourceSupport(queryModelResourceSupport)
                                .node(select)
                                .sourceCounter(nextSourceIndex)
                                .targetCounter(nextTargetIndex)
                                .variables(Collections.singletonMap(JqlExpressionBuilder.SELF_NAME, select)).build();
                        addAttributes(node.get(), context, select.getMainTarget()); // add all attributes to logical query
                        addFilter(node.get(), context);
                    } else {
                        log.error("No root node of expression tree found for mapped transfer object type: {}, query not created", AsmUtils.getClassifierFQName(mappedTransferObjectType));
                    }
                });

        // add references (both single and multiple)
        getAsmElement(EClass.class)
                .filter(c -> asmUtils.getMappedEntityType(c).isPresent())
                .forEach(mappedTransferObjectType -> {
                    final Optional<MappedTransferObjectTypeBindings> node = transferObjectTypeBindingsCollector.getTransferObjectGraph(mappedTransferObjectType);

                    if (node.isPresent()) {
                        if (log.isTraceEnabled()) {
                            log.trace("Adding references of transfer object type: {}", AsmUtils.getClassifierFQName(mappedTransferObjectType));
                        }
                        final Select select = transferObjectQueries.get(mappedTransferObjectType);
                        addReferences(node.get(), Context.builder()
                                        .queryModelResourceSupport(queryModelResourceSupport)
                                        .node(select)
                                        .sourceCounter(nextSourceIndex)
                                        .targetCounter(nextTargetIndex)
                                        .variables(Collections.singletonMap(JqlExpressionBuilder.SELF_NAME, select)).build(),
                                select.getMainTarget(),
                                ECollections.emptyEList()); // add all references to logical query
                    } else {
                        log.error("No root node of expression tree found for mapped transfer object type: {}, reference not added", AsmUtils.getClassifierFQName(mappedTransferObjectType));
                    }
                });
    }

    private void createNavigations() {
        getAsmElement(EAttribute.class)
                .filter(a -> a.isDerived() && !AsmUtils.isEntityType(a.getEContainingClass()) && isStaticAttribute(a))
                .forEach(attribute -> {
                    final DataExpression dataExpression;
                    if (asmUtils.isMappedTransferObjectType(attribute.getEContainingClass())) {
                        if (attribute.getEContainingClass().getEAllAttributes().stream()
                                .anyMatch(a -> Objects.equals(attribute.getName(), AsmUtils.getExtensionAnnotationValue(a, "default", false).orElse("-")))) {
                            dataExpression = asmUtils.getMappedAttribute(attribute).map(mappedAttribute ->
                                    getEntityTypeExpressionsMap().get(mappedAttribute.getEContainingClass()).getGetterAttributeExpressions().get(mappedAttribute))
                                    .orElse(null);
                        } else {
                            // attribute is not used as default
                            return;
                        }
                    } else {
                        final Optional<UnmappedTransferObjectTypeBindings> source = transferObjectTypeBindingsCollector.getTransferObjectBindings(attribute.getEContainingClass());
                        if (!source.isPresent()) {
                            log.warn("Expression binding not found: {}", AsmUtils.getAttributeFQName(attribute));
                            return;
                        }

                        dataExpression = source.get().getDataExpressions().get(attribute);
                    }

                    if (dataExpression == null) {
                        log.error("Data expression is not defined for attribute: {}", AsmUtils.getAttributeFQName(attribute));
                        return;
                    }

                    final EAttribute resultAttribute = EAttributeBuilder.create()
                            .withName(attribute.getName())
                            .withEType(attribute.getEAttributeType())
                            .build();
                    final EClass resultType = EClassBuilder.create()
                            .withName("__ResultType")
                            .withEStructuralFeatures(resultAttribute)
                            .build();

                    final Select select = newSelectBuilder()
                            .withAlias(DATA_ALIAS)
                            .build();
                    final Target mainTarget = newTargetBuilder()
                            .withType(resultType)
                            .withIndex(nextTargetIndex.incrementAndGet())
                            .withNode(select)
                            .build();
                    final Feature feature = featureFactory.convert(dataExpression, Context.builder()
                                    .queryModelResourceSupport(queryModelResourceSupport)
                                    .node(select)
                                    .sourceCounter(nextSourceIndex)
                                    .targetCounter(nextTargetIndex)
                                    .variables(Collections.emptyMap())
                                    .build(),
                            newFeatureTargetMappingBuilder()
                                    .withTarget(mainTarget)
                                    .withTargetAttribute(resultAttribute)
                                    .build());

                    useSelect(select)
                            .withFeatures(feature)
                            .withTargets(mainTarget)
                            .withMainTarget(mainTarget)
                            .build();

                    final SubSelect subSelect = newSubSelectBuilder()
                            .withSelect(select)
                            .withEmbeddedSelect(select)
                            .withAlias(EXPOSED_ALIAS)
                            .build();
                    queryModelResourceSupport.addContent(subSelect);
                    queryModelResourceSupport.addContent(resultType);

                    dataQueries.put(attribute, subSelect);
                });

        getAsmElement(EReference.class)
                .filter(r -> asmUtils.isMappedTransferObjectType(r.getEReferenceType()) && r.isDerived() && !AsmUtils.isEntityType(r.getEContainingClass()) && isStaticReference(r))
                .forEach(navigation -> {
                    final ReferenceExpression selector;
                    if (asmUtils.isMappedTransferObjectType(navigation.getEContainingClass())) {
                        if (navigation.getEContainingClass().getEAllReferences().stream()
                                .anyMatch(a -> Objects.equals(navigation.getName(), AsmUtils.getExtensionAnnotationValue(a, "default", false).orElse("-")))) {
                            selector = asmUtils.getMappedReference(navigation).map(mappedReference ->
                                    getEntityTypeExpressionsMap().get(mappedReference.getEContainingClass()).getGetterReferenceExpressions().get(mappedReference))
                                    .orElse(null);
                        } else {
                            // reference is not used as default
                            return;
                        }
                    } else {
                        final Optional<UnmappedTransferObjectTypeBindings> source = transferObjectTypeBindingsCollector.getTransferObjectBindings(navigation.getEContainingClass());
                        if (!source.isPresent()) {
                            log.warn("Expression binding not found: {}", AsmUtils.getReferenceFQName(navigation));
                            return;
                        }

                        selector = source.get().getNavigationExpressions().get(navigation);
                    }

                    if (selector == null) {
                        log.error("Selector is not defined for navigation: {}", AsmUtils.getReferenceFQName(navigation));
                        return;
                    }

                    final SubSelect subSelect = newSubSelectBuilder()
                            .withSelect(getQuery(navigation.getEReferenceType()).get())
                            .withAlias(EXPOSED_ALIAS)
                            .withTransferRelation(navigation)
                            .build();

                    final JoinFactory.PathEnds pathEnds = joinFactory.convertNavigationToJoins(Context.builder()
                                    .queryModelResourceSupport(queryModelResourceSupport)
                                    .sourceCounter(nextSourceIndex)
                                    .targetCounter(nextTargetIndex)
                                    .variables(Collections.emptyMap()).build(),
                            subSelect,
                            selector, false);

                    if (log.isTraceEnabled()) {
                        log.trace("Static navigation: " + subSelect.getNavigationJoins().stream().map(j -> j.toString()).collect(Collectors.joining(", ")));
                    }

                    if (pathEnds.isOrdered()) {
                        orderedTransferRelations.add(navigation);
                    }
                    queryModelResourceSupport.addContent(subSelect);

                    navigationQueries.put(navigation, subSelect);
                });
    }

    public Feature dataExpressionToFeature(final DataExpression dataExpression, final Context context, final Target target, final EAttribute targetAttribute) {
        if (log.isTraceEnabled()) {
            log.trace(pad(context.getVariables().size()) + "- converting expression {} of {} to feature {} of {}", new Object[]{dataExpression, context.getVariables().get(JqlExpressionBuilder.SELF_NAME).getAlias(), targetAttribute != null ? targetAttribute.getName() : "undefined", target != null ? target : "undefined"});
        }
        return featureFactory.convert(dataExpression, context, target != null && targetAttribute != null ? newFeatureTargetMappingBuilder()
                .withTarget(target)
                .withTargetAttribute(targetAttribute)
                .build() : null);
    }

    /**
     * Add all attributes of a given expression tree node to logical query, returning result set in a target.
     *
     * @param mappedTransferObjectTypeBindings node of expression tree
     * @param context                          context
     * @param target                           projection of node added to logical query
     */
    private void addAttributes(final MappedTransferObjectTypeBindings mappedTransferObjectTypeBindings, final Context context, final Target target) {
        if (!AsmUtils.equals(mappedTransferObjectTypeBindings.getTransferObjectType(), target.getType())) {
            throw new IllegalArgumentException("Mismatching target type");
        }
        log.trace("  - adding attributes of {} to {}", context.getVariables().get(JqlExpressionBuilder.SELF_NAME).getAlias(), target);
        mappedTransferObjectTypeBindings.getGetterAttributeExpressions().entrySet().stream()
                .filter(e -> e.getKey().getEContainingClass().getEAllStructuralFeatures().stream()
                        .noneMatch(f -> AsmUtils.getExtensionAnnotationValue(f, "default", false)
                                .filter(d -> Objects.equals(d, e.getKey().getName()))
                                .isPresent()))
                .forEach(e -> dataExpressionToFeature(e.getValue(), context, target, e.getKey()));
    }

    private void addFilter(final MappedTransferObjectTypeBindings mappedTransferObjectTypeBindings, final Context context) {
        final Node self = context.getVariables().get(JqlExpressionBuilder.SELF_NAME);
        if (mappedTransferObjectTypeBindings.getFilter() != null) {
            log.trace("  - adding filter of {}", self.getAlias());

            final Feature feature = dataExpressionToFeature(mappedTransferObjectTypeBindings.getFilter(), context, null, null);
            self.getFilters().add(newFilterBuilder()
                    .withAlias("f" + self.getAlias())
                    .withFeature(feature)
                    .build());
        }
    }

    /**
     * Add all references (both single and multiple) of a given expression tree node to logical query.
     *
     * @param mappedTransferObjectTypeBindings node of expression tree
     * @param context                          context
     */
    private void addReferences(final MappedTransferObjectTypeBindings mappedTransferObjectTypeBindings, final Context context, final Target target, final EList<EReference> referenceChain) {
        final EList<EReference> subSelectReferences = new UniqueEList<>();

        mappedTransferObjectTypeBindings.getReferences().entrySet().stream()
                .filter(c -> mappedTransferObjectTypeBindings.getGetterReferenceExpressions().containsKey(c.getKey()))
                .forEach(c -> {
                    EReference reference = c.getKey();

                    final EList<EReference> newReferenceChain = new BasicEList<>();
                    newReferenceChain.addAll(referenceChain);
                    newReferenceChain.add(reference);

                    if (log.isTraceEnabled()) {
                        log.trace(pad(referenceChain.size()) + "- adding SUBQUERY reference: {}", newReferenceChain.stream().map(r -> r.getName()).collect(Collectors.joining(".")));
                    }

                    // create SUBSELECT for multiple reference including logical query of the reference
                    final SubSelect subSelect = newSubSelectBuilder()
                            .withAlias(getNextSubSelectAlias(nextSourceIndex))
                            .withSelect(transferObjectQueries.get(reference.getEReferenceType()))
                            .withTransferRelation(reference)
                            .build();

                    if (customJoinDefinitions.get(reference) != null && reference.isDerived()) {
                        final EClass entityType = asmUtils.getMappedEntityType(reference.getEReferenceType()).orElseThrow(() -> new IllegalStateException("Entity type not found"));

                        final CustomJoin customJoin = newCustomJoinBuilder()
                                .withAlias("custom" + context.getSourceCounter().incrementAndGet())
                                .withPartner(context.getNode())
                                .withTransferRelation(reference)
                                .withType(entityType)
                                .withNavigationSql(customJoinDefinitions.get(reference).getNavigationSql())
                                .build();
                        if (customJoinDefinitions.get(reference).getSourceIdParameterName() != null) {
                            customJoin.setSourceIdParameter(customJoinDefinitions.get(reference).getSourceIdParameterName());
                        }
                        if (customJoinDefinitions.get(reference).getSourceIdSetParameterName() != null) {
                            customJoin.setSourceIdSetParameter(customJoinDefinitions.get(reference).getSourceIdSetParameterName());
                        }
                        subSelect.setBase(context.getNode());
                        subSelect.getJoins().add(customJoin);
                        subSelect.setPartner(customJoin);
                    } else {
                        // add additional JOINs that specifies the role (navigation expressions of navigation properties)
                        final ReferenceExpression referenceExpression = mappedTransferObjectTypeBindings.getGetterReferenceExpressions().get(reference);
                        final JoinFactory.PathEnds pathEnds = joinFactory.convertNavigationToJoins(context, subSelect, referenceExpression, false);
                        if (pathEnds.isOrdered()) {
                            orderedTransferRelations.add(reference);
                        }
                    }

                    final boolean includedByJoin = !reference.isMany() &&
                                                   !referenceChain.contains(reference) &&
                                                   !subSelect.getNavigationJoins().stream().anyMatch(j -> j instanceof SubSelectJoin && isCircularAggregation(referenceChain, reference)) && // TODO - check condition is necessary
                                                   AsmUtils.equals(subSelect.getBase(), context.getNode());
                    subSelect.setExcluding(includedByJoin);

                    if (!includedByJoin) {
                        // add main target to the reference to target list of base select
                        final Target subSelectTarget = subSelect.getSelect().getMainTarget();
                        target.getReferencedTargets().add(newReferencedTargetBuilder()
                                .withReference(reference)
                                .withTarget(subSelectTarget)
                                .build());
                        subSelectReferences.add(reference);
                    }

                    context.getVariables().get(JqlExpressionBuilder.SELF_NAME).getSubSelects().add(subSelect);
                });

        // single references are added as JOIN
        mappedTransferObjectTypeBindings.getReferences().entrySet().stream()
                .filter(c -> AsmUtils.isEmbedded(c.getKey()) && !subSelectReferences.contains(c.getKey()) && mappedTransferObjectTypeBindings.getGetterReferenceExpressions().containsKey(c.getKey()))
                .forEach(c -> {
                    EReference reference = c.getKey();
                    MappedTransferObjectTypeBindings transferObjectTypeBindings = c.getValue();

                    final EList<EReference> newReferenceChain = new BasicEList<>();
                    newReferenceChain.addAll(referenceChain);
                    newReferenceChain.add(reference);

                    if (log.isTraceEnabled()) {
                        log.trace(pad(referenceChain.size()) + "- adding JOIN reference: {}", newReferenceChain.stream().map(r -> r.getName()).collect(Collectors.joining(".")));
                    }

                    // get logical query of single reference, several elements will be copied from it
                    final Select joinedSelect = transferObjectQueries.get(reference.getEReferenceType());

                    final Node self = context.getVariables().get(JqlExpressionBuilder.SELF_NAME);
                    final Node nextNode;

                    if (customJoinDefinitions.get(reference) != null && reference.isDerived()) {
                        final EClass entityType = asmUtils.getMappedEntityType(reference.getEReferenceType()).orElseThrow(() -> new IllegalStateException("Entity type not found"));

                        final CustomJoin customJoin = newCustomJoinBuilder()
                                .withAlias("custom" + context.getSourceCounter().incrementAndGet())
                                .withPartner(context.getNode())
                                .withTransferRelation(reference)
                                .withType(entityType)
                                .withNavigationSql(customJoinDefinitions.get(reference).getNavigationSql())
                                .build();
                        if (customJoinDefinitions.get(reference).getSourceIdParameterName() != null) {
                            customJoin.setSourceIdParameter(customJoinDefinitions.get(reference).getSourceIdParameterName());
                        }
                        if (customJoinDefinitions.get(reference).getSourceIdSetParameterName() != null) {
                            customJoin.setSourceIdSetParameter(customJoinDefinitions.get(reference).getSourceIdSetParameterName());
                        }
                        self.getJoins().add(customJoin);

                        nextNode = newCastJoinBuilder()
                                .withAlias(QueryUtils.getNextJoinAlias(context.getSourceCounter()))
                                .withPartner(customJoin)
                                .withType(entityType)
                                .build();
                        self.getJoins().add((Join) nextNode);
                    } else {
                        // add additional JOINs that specifies the role (navigation expressions of navigation properties)
                        final ReferenceExpression referenceExpression = mappedTransferObjectTypeBindings.getGetterReferenceExpressions().get(reference);
                        final JoinFactory.PathEnds pathEnds = joinFactory.convertNavigationToJoins(context, self, referenceExpression, false);
                        if (pathEnds.isOrdered()) {
                            orderedTransferRelations.add(reference);
                        }
                        nextNode = pathEnds.getPartner();
                    }

                    final Target joinedTarget = newTargetBuilder()
                            .withType(joinedSelect.getMainTarget().getType())
                            .withIndex(nextTargetIndex.incrementAndGet())
                            .build(); // set target alias (excluded from SQL)
                    joinedTarget.setContainerWithIdFeature(nextNode, true);

                    if (self instanceof Select) {
                        ((Select) self).getTargets().add(joinedTarget);
                    } else if (self instanceof Join) {
                        ((Select) ((Join) self).getBase()).getTargets().add(joinedTarget);
                    } else {
                        throw new IllegalStateException("Invalid variable");
                    }

                    // add main target of the reference to target list of base select
                    target.getReferencedTargets().add(newReferencedTargetBuilder()
                            .withReference(reference)
                            .withTarget(joinedTarget)
                            .build());

                    addReferences(transferObjectTypeBindings, context.clone(JqlExpressionBuilder.SELF_NAME, nextNode), joinedTarget, newReferenceChain);
                    addAttributes(transferObjectTypeBindings, context.clone(JqlExpressionBuilder.SELF_NAME, nextNode), joinedTarget); // add joined attributes to logical query
                    addFilter(transferObjectTypeBindings, context.clone(JqlExpressionBuilder.SELF_NAME, nextNode));
                });
    }

    public boolean isStaticReference(EReference reference) {
        return asmUtils.getMappedReference(reference)
                .map(r -> Boolean.TRUE.equals(transferObjectTypeBindingsCollector.isStaticReference(r)))
                .orElse(true); // static=true returned for transfer object relations without binding
    }

    public boolean isStaticAttribute(EAttribute attribute) {
        return asmUtils.getMappedAttribute(attribute)
                .map(a -> Boolean.TRUE.equals(transferObjectTypeBindingsCollector.isStaticAttribute(a)))
                .orElse(true); // static=true returned for transfer attributes without binding
    }

    private boolean isCircularAggregation(final EList<EReference> references, final EReference reference) {
        final EList<EClass> sourceTypes = new UniqueEList<>();
        sourceTypes.addAll(references.stream()
                .map(r -> r.getEContainingClass())
                .collect(Collectors.toList()));
        sourceTypes.add(reference.getEContainingClass());

        return isCircularAggregation(sourceTypes, new UniqueEList<>(), ECollections.singletonEList(reference));
    }

    private boolean isCircularAggregation(final EList<EClass> sourceTypes, final EList<EClass> checkedTypes, final EList<EReference> references) {
        if (references.stream()
                .map(r -> r.getEReferenceType())
                .anyMatch(t -> sourceTypes.contains(t))) {
            return true;
        }

        final EList<EClass> nextTypes = new UniqueEList<>();
        nextTypes.addAll(references.stream()
                .filter(r -> !checkedTypes.contains(r.getEReferenceType()))
                .map(r -> r.getEReferenceType())
                .collect(Collectors.toList()));

        final EList<EReference> nextReferences = new UniqueEList<>();
        nextReferences.addAll(nextTypes.stream()
                .flatMap(c -> c.getEAllReferences().stream()
                        .filter(r -> AsmUtils.isEmbedded(r)))
                .collect(Collectors.toList()));
        checkedTypes.addAll(nextTypes);

        if (nextReferences.isEmpty()) {
            return false;
        } else {
            return isCircularAggregation(sourceTypes, checkedTypes, nextReferences);
        }
    }

    /**
     * Get elements with a given type of ASM metamodel.
     *
     * @param clazz class of element
     * @param <T>   element type
     * @return stream of elements
     */
    @SuppressWarnings("unchecked")
	<T> Stream<T> getAsmElement(final Class<T> clazz) {
        final Iterable<Notifier> asmContents = asmResourceSet::getAllContents;
        return StreamSupport.stream(asmContents.spliterator(), false)
                .filter(e -> clazz.isAssignableFrom(e.getClass())).map(e -> (T) e);
    }

    private static String pad(int level) {
        return StringUtils.leftPad("", level * 2, " ");
    }
}
