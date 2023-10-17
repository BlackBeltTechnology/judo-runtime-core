package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

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

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor.FilterJoinProcessorParameters;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor.JoinProcessParameters;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.*;
import java.util.stream.*;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil.AGGREGATE_PREFIX;
import static hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil.getParentIdColumnAlias;

@Slf4j
public class RdbmsResultSet<ID> extends RdbmsField {

    private SubSelect query;

    // map of ancestors (the are holder of an attribute) of a given source
    private final EMap<Node, EList<EClass>> ancestors = ECollections.asEMap(new HashMap<>());

    private final EMap<Node, EList<EClass>> descendants = ECollections.asEMap(new HashMap<>());

    private final String from;
    private final Collection<RdbmsField> columns = new ArrayList<>();
    private final List<RdbmsJoin> joins = new ArrayList<>();
    private final List<RdbmsField> conditions = new ArrayList<>();
    private final List<RdbmsOrderBy> orderBys = new ArrayList<>();

    private final EMap<OrderBy, RdbmsField> orderByFeatures = ECollections.asEMap(new LinkedHashMap<>());

    private Integer limit = null;
    private Integer offset = null;

    private final boolean group;
    private final boolean skipParents;

    private final EList<Join> processedNodesForJoins = ECollections.newBasicEList();

    private final RdbmsBuilder<ID> rdbmsBuilder;

    @Getter
    private final Set<String> joinConditionTableAliases = new HashSet<>();

    @Builder
    private RdbmsResultSet(
            @NonNull final SubSelect query,
            final RdbmsBuilderContext builderContext,
            final boolean filterByInstances,
            final DAO.Seek seek,
            final boolean withoutFeatures,
            final Map<String, Object> mask,
            final boolean skipParents) {
        this.query = query;
        this.skipParents = skipParents;
        this.rdbmsBuilder = (RdbmsBuilder<ID>) builderContext.getRdbmsBuilder();

        if (log.isTraceEnabled()) {
            log.trace("Query:              " + query.toString());
            log.trace("Filter by instance: " + filterByInstances);
            log.trace("Without features:   " + withoutFeatures);
            log.trace("Mask:               " + (mask == null ? "" : withoutFeatures));
            log.trace("Skip parents:       " + skipParents);
            log.trace("Builder context:    " + builderContext.toString());
        }
        RdbmsBuilderContext resultSetBuilderContext = builderContext.toBuilder()
                .ancestors(ancestors)
                .descendants(descendants)
                .build();

        final EClass type = query.getSelect().getType();
        from = type != null ? rdbmsBuilder.getTableName(type) : null;

        final EMap<Target, Collection<String>> targetMask = mask != null ? getTargetMask(query.getSelect().getMainTarget(), mask, new BasicEMap<>()) : ECollections.emptyEMap();
        final List<Feature> features = query.getSelect().getFeatures().stream()
                .filter(
                        f -> !f.getTargetMappings().isEmpty() &&
                                (!withoutFeatures &&
                                        isFeatureIncludedByMask(f, mask, targetMask) ||
                                        query.getSelect().isAggregated() ||
                                        isFeatureId(f) ||
                                        isFeatureType(f)
                                )
                ).collect(Collectors.toList());

        for (Feature feature : features) {
            List<RdbmsField> featureFields = rdbmsBuilder.mapFeatureToRdbms(feature, resultSetBuilderContext.toBuilder().parentIdFilterQuery(query).build())
                    .collect(Collectors.toList());
            columns.addAll(featureFields);
        }
        
        rdbmsBuilder.addAncestorJoins(joins, query.getSelect(), ancestors, resultSetBuilderContext);

        List<Join> joinsToProcess = query.getSelect().getAllJoins().stream()
                .filter(j -> !withoutFeatures || query.getSelect().isAggregated() && !processedNodesForJoins.contains(j))
                .collect(Collectors.toList());

        for (Join join : joinsToProcess) {
            joins.addAll(rdbmsBuilder.processJoin(
                    JoinProcessParameters.builder()
                            .join(join)
                            .builderContext(resultSetBuilderContext)
                            .withoutFeatures(withoutFeatures)
                            .build()
            ));
            processedNodesForJoins.add(join);
        }

        if (filterByInstances) {
            conditions.add(RdbmsFunction.builder()
                    .pattern("{0} IN ({1})")
                    .parameters(Arrays.asList(
                            RdbmsColumn.builder().partnerTable(query.getSelect()).columnName(StatementExecutor.ID_COLUMN_NAME).build(),
                            RdbmsParameter.builder().parameterName(RdbmsAliasUtil.getInstanceIdsKey(query.getSelect())).build()
                    )).build());
        }

        final Collection<SubSelect> subSelects = Stream.concat(
                query.getSelect().getSubSelects().stream(),
                query.getSelect().getAllJoins().stream().flatMap(j -> j.getSubSelects().stream())
        ).collect(Collectors.toList());

        if (!withoutFeatures || query.getSelect().isAggregated()) {
            joins.addAll(subSelects.stream()
                    .filter(s -> s.getSelect().isAggregated())
                    .filter(s ->
                            query.getSelect().getFeatures().stream().anyMatch(f ->
                                    f.getNodes().stream().anyMatch(n ->
                                            AsmUtils.equals(n, s.getSelect()) || s.getSelect().getJoins().contains(n))))
                    .map(s -> RdbmsQueryJoin.<ID>builder()
                            .resultSet(
                                    RdbmsResultSet.<ID>builder()
                                            .query(s)
                                            .builderContext(resultSetBuilderContext)
                                            .withoutFeatures(withoutFeatures)
                                            .build())
                            .outer(true)
                            .columnName(RdbmsAliasUtil.getOptionalParentIdColumnAlias(s.getContainer()))
                            .partnerTable(s.getNavigationJoins().isEmpty() ? null : s.getContainer())
                            .partnerColumnName(s.getNavigationJoins().isEmpty() ? null : StatementExecutor.ID_COLUMN_NAME)
                            .alias(s.getAlias())
                            .build())
                    .collect(Collectors.toList()));
        }

        if (query.getSelect().isAggregated() && !query.getNavigationJoins().isEmpty()) {
            Node n = query.getContainer();
            final EList<Node> nodes = new BasicEList<>();
            while (n != null) {
                nodes.add(n);
                if (n instanceof SubSelectJoin) {
                    nodes.add(((SubSelectJoin) n).getSubSelect().getBase());
                }
                n = (Node) n.eContainer();
            }
            group = query.getNavigationJoins().get(0).getPartner() != null && nodes.contains(query.getNavigationJoins().get(0).getPartner());
        } else {
            group = false;
        }

        if (!query.getNavigationJoins().isEmpty()) {
            if (query.getContainer() != null && (group || !query.getSelect().isAggregated()) && !skipParents && !query.getSelect().isSingleColumnedSelect()) {
                // add parent ID to result set that will be used to move result records under their container records
                columns.add(RdbmsColumn.builder()
                        .partnerTablePrefix(AGGREGATE_PREFIX)
                        .partnerTable(query)
                        .columnName(getParentIdColumnAlias(query.getContainer()))
                        .alias(getParentIdColumnAlias(query.getContainer()))
                        .build());
            }

            final RdbmsNavigationJoin<ID> navigationJoin =
                    RdbmsNavigationJoin.<ID>builder()
                            .builderContext(builderContext)
                            .query(query)
                            .withoutFeatures(withoutFeatures)
                            .build();
            orderBys.addAll(navigationJoin.getExposedOrderBys());

            joins.add(navigationJoin);
        } else {
            for (OrderBy orderBy : query.getOrderBys()) {
                joins.add(RdbmsTableJoin.builder()
                        .tableName(rdbmsBuilder.getTableName(orderBy.getType()))
                        .columnName(StatementExecutor.ID_COLUMN_NAME)
                        .partnerTable(query.getSelect())
                        .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                        .alias(orderBy.getAlias())
                        .build());

                List<Join> orderByJoins = orderBy.getFeature().getNodes().stream()
                        .filter(n -> n instanceof Join).map(n -> (Join) n)
                        .filter(n -> !AsmUtils.equals(n, query) && !AsmUtils.equals(n, query.getSelect()) && !query.getSelect().getAllJoins().contains(n))
                        .collect(Collectors.toList());

                for (Join join : orderByJoins) {
                    joins.addAll(rdbmsBuilder.processJoin(
                            JoinProcessParameters.builder()
                                    .builderContext(resultSetBuilderContext)
                                    .join(join)
                                    .withoutFeatures(withoutFeatures)
                                    .build()
                    ));
                    processedNodesForJoins.add(join);
                }

                final List<RdbmsField> orderByFields = rdbmsBuilder.mapFeatureToRdbms(orderBy.getFeature(), resultSetBuilderContext).collect(Collectors.toList());
                if (!orderByFields.isEmpty()) {
                    orderByFeatures.put(orderBy, orderByFields.get(0));
                }

                final List<RdbmsOrderBy> newOrderBys = orderByFields.stream()
                        .map(o -> RdbmsOrderBy.builder()
                                .rdbmsField(o)
                                .descending(orderBy.isDescending())
                                .build())
                        .collect(Collectors.toList());
                columns.addAll(newOrderBys.stream().map(o -> o.getRdbmsField()).collect(Collectors.toList()));
                orderBys.addAll(newOrderBys);
            }
        }

        limit = query.getLimit();
        offset = query.getOffset();

        for (OrderBy orderBy : query.getSelect().getOrderBys()) {
            final List<RdbmsField> orderByFields = rdbmsBuilder.mapFeatureToRdbms(orderBy.getFeature(), resultSetBuilderContext).collect(Collectors.toList());
            if (!orderByFields.isEmpty()) {
                orderByFeatures.put(orderBy, orderByFields.get(0));
            }

            final List<RdbmsOrderBy> newOrderBys = orderByFields.stream()
                    .map(o -> RdbmsOrderBy.builder()
                            .rdbmsField(o)
                            .descending(orderBy.isDescending())
                            .build())
                    .collect(Collectors.toList());

            orderBys.addAll(newOrderBys);
        }

        final boolean addJoinOfFilterFeature =
                (query.getBase() != null &&
                        !(query.getBase() instanceof Select &&
                                !(query.getContainer() instanceof SubSelectJoin) &&
                                query.getBase().getFeatures().isEmpty() && query.getSelect().isAggregated())) ||
                        (query.getPartner() == null && query.eContainer() == null);

        for (Filter filter : query.getFilters()) {
            rdbmsBuilder.addFilterJoinsAndConditions(
                    FilterJoinProcessorParameters.builder()
                            .joins(joins)
                            .conditions(conditions)
                            .processedNodesForJoins(processedNodesForJoins)
                            .query(query)
                            .filter(filter)
                            .partnerTable(query.getPartner() != null ? query : query.getSelect())
                            .partnerTablePrefix(query.getPartner() != null ? AGGREGATE_PREFIX : "")
                            .addJoinsOfFilterFeature(addJoinOfFilterFeature)
                            .build(), resultSetBuilderContext);
        }

        for (Filter filter : query.getSelect().getFilters()) {
            rdbmsBuilder.addFilterJoinsAndConditions(
                    FilterJoinProcessorParameters.builder()
                            .joins(joins)
                            .conditions(conditions)
                            .processedNodesForJoins(processedNodesForJoins)
                            .query(query)
                            .filter(filter)
                            .partnerTable(query.getSelect())
                            .addJoinsOfFilterFeature(true)
                            .build(), resultSetBuilderContext);
        }

        if (limit != null && seek != null && seek.getLastItem() != null && from != null) {
            orderByFeatures.stream()
                    .findFirst().ifPresent(orderBy -> {
                        final Object value = getLastItemValue(rdbmsBuilder, query.getSelect().getMainTarget().getType(), seek, orderBy.getKey());

                        if (value != null) {
                            conditions.add(RdbmsFunction.builder()
                                    .pattern(orderBy.getKey().isDescending() ? "{0} <= {1}" : "({0} >= {1} OR {0} IS NULL)")
                                    .parameter(orderBy.getValue())
                                    .parameter(RdbmsNamedParameter.builder()
                                            .parameter(rdbmsBuilder.getParameterMapper().createParameter(value, null))
                                            .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                                            .build())
                                    .build());
                        } else if (!orderBy.getKey().isDescending()) {
                            conditions.add(RdbmsFunction.builder()
                                    .pattern("{0} IS NULL")
                                    .parameter(orderBy.getValue())
                                    .build());
                        }
                    });
        }

        if (limit != null && seek != null && seek.getLastItem() != null) {
            final Object lastId = rdbmsBuilder.getCoercer().coerce(seek.getLastItem().get(rdbmsBuilder.getIdentifierProvider().getName()), rdbmsBuilder.getIdentifierProvider().getType());
            checkArgument(lastId != null, "Missing identifier from last item");

            final Collection<RdbmsField> conditionOrFragments = new ArrayList<>();
            for (int index = 0; index <= orderByFeatures.size(); index++) {
                final Collection<RdbmsField> conditionAndFragments = new ArrayList<>();

                final Iterator<Map.Entry<OrderBy, RdbmsField>> nestedIt = orderByFeatures.entrySet().iterator();
                for (int i = 0; i < index; i++) {
                    final Map.Entry<OrderBy, RdbmsField> nestedItem = nestedIt.next();
                    final Object value = getLastItemValue(rdbmsBuilder, query.getSelect().getMainTarget().getType(), seek, nestedItem.getKey());

                    if (value != null) {
                        conditionAndFragments.add(RdbmsFunction.builder()
                                .pattern("{0} = {1}")
                                .parameter(nestedItem.getValue())
                                .parameter(RdbmsNamedParameter.builder()
                                        .parameter(rdbmsBuilder.getParameterMapper().createParameter(value, null))
                                        .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                                        .build())
                                .build());
                    } else {
                        conditionAndFragments.add(RdbmsFunction.builder()
                                .pattern("{0} IS NULL")
                                .parameter(nestedItem.getValue())
                                .build());
                    }
                }

                if (nestedIt.hasNext()) {
                    final Map.Entry<OrderBy, RdbmsField> nestedItem = nestedIt.next();
                    final Object value = getLastItemValue(rdbmsBuilder, query.getSelect().getMainTarget().getType(), seek, nestedItem.getKey());

                    if (value != null) {
                        conditionAndFragments.add(RdbmsFunction.builder()
                                .pattern(nestedItem.getKey().isDescending() ? "{0} < {1}" : "({0} > {1} OR {0} IS NULL)")
                                .parameter(nestedItem.getValue())
                                .parameter(RdbmsNamedParameter.builder()
                                        .parameter(rdbmsBuilder.getParameterMapper().createParameter(value, null))
                                        .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                                        .build())
                                .build());
                    } else if (nestedItem.getKey().isDescending()) {
                        conditionAndFragments.add(RdbmsFunction.builder()
                                .pattern("{0} IS NOT NULL")
                                .parameter(nestedItem.getValue())
                                .build());
                    }
                } else {
                    conditionAndFragments.add(RdbmsFunction.builder()
                            .pattern("{0} " + (seek.isReverse() ? " < " : " > ") + "{1}")
                            .parameter(RdbmsColumn.builder()
                                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                                    .partnerTable(query.getSelect())
                                    .build())
                            .parameter(RdbmsNamedParameter.builder()
                                    .parameter(rdbmsBuilder.getParameterMapper().createParameter(lastId, null))
                                    .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                                    .build())
                            .build());
                }

                if (!conditionAndFragments.isEmpty()) {
                    conditionOrFragments.add(RdbmsFunction.builder()
                            .pattern(IntStream.range(0, conditionAndFragments.size()).mapToObj(i -> "{" + i + "}").collect(Collectors.joining(" AND ")))
                            .parameters(conditionAndFragments)
                            .build());
                }
            }

            conditions.add(RdbmsFunction.builder()
                    .pattern(IntStream.range(0, conditionOrFragments.size()).mapToObj(i -> "{" + i + "}").collect(Collectors.joining(" OR ")))
                    .parameters(conditionOrFragments)
                    .build());
        }

        if (seek != null) {
            orderBys.add(RdbmsOrderBy.builder()
                    .rdbmsField(RdbmsColumn.builder()
                            .columnName(StatementExecutor.ID_COLUMN_NAME)
                            .target(query.getSelect().getMainTarget())
                            .partnerTable(query.getSelect())
                            .build())
                    .descending(seek.isReverse())
                    .build());
        }
    }

    private EMap<Target, Collection<String>> getTargetMask(final Target target, final Map<String, Object> mask, final EMap<Target, Collection<String>> result) {
        result.put(target, mask.entrySet().stream()
                .filter(e -> target.getType().getEAllAttributes().stream().anyMatch(a -> Objects.equals(a.getName(), e.getKey())))
                .map(e -> e.getKey())
                .collect(Collectors.toList()));

        Collection<ReferencedTarget> referencedTargets =
        mask.entrySet().stream()
                .filter(e -> target.getType().getEAllReferences().stream().anyMatch(r -> Objects.equals(r.getName(), e.getKey())))
                .map(e -> target.getReferencedTargets().stream()
                        .filter(rt -> Objects.equals(rt.getReference().getName(), e.getKey()))
                        .findAny().orElse(null))
                .filter(rt -> rt != null && !result.containsKey(rt.getTarget()))
                .collect(Collectors.toList());

        for (ReferencedTarget rt : referencedTargets) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> _mask = (Map<String, Object>) mask.get(rt.getReference().getName());
            result.putAll(getTargetMask(rt.getTarget(), _mask, result));
        }
        return result;
    }

    private Object getLastItemValue(final RdbmsBuilder<ID> rdbmsBuilder, final EClass type, final DAO.Seek seek, final OrderBy orderBy) {
        if (!orderBy.getFeature().getTargetMappings().isEmpty()) {
            return seek.getLastItem().get(orderBy.getFeature().getTargetMappings().get(0).getTargetAttribute().getName());
        } else if (orderBy.getFeature().getTargetMappings().isEmpty() && orderBy.getFeature() instanceof Attribute) {
            final EAttribute entityAttribute = ((Attribute) orderBy.getFeature()).getSourceAttribute();
            final EAttribute transferAttribute = type.getEAllAttributes().stream()
                    .filter(a -> AsmUtils.equals(entityAttribute, rdbmsBuilder.getAsmUtils().getMappedAttribute(a).orElse(null)))
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("Unable to find order by attribute of last item"));
            return seek.getLastItem().get(transferAttribute.getName());
        } else {
            throw new IllegalArgumentException("Unable to find order by attribute to last item");
        }
    }


    @Override
    public String toSql(SqlConverterContext context) {
        final String prefix = context.prefix;
        final EMap<Node, String> prefixes = context.prefixes;

        final EMap<Node, String> newPrefixes = new BasicEMap<>();
        newPrefixes.putAll(prefixes);
        newPrefixes.put(query.getSelect(), prefix);
        newPrefixes.putAll(query.getSelect().getAllJoins().stream()
                .collect(Collectors.toMap(join -> join, join -> prefix)));

        SqlConverterContext resultContext = context.toBuilder()
                .prefixes(newPrefixes)
                .build();

        final Collection<String> allConditions = Stream
                .concat(
                        joins.stream().flatMap(j -> j.conditionToSql(resultContext).stream()),
                        conditions.stream().map(c -> c.toSql(resultContext.toBuilder().includeAlias(false).build()))
                )
                .collect(Collectors.toList());

        final List<String> orderByAttributes = orderBys.stream().map(o -> o.toSql(context)).collect(Collectors.toList());

        boolean multiplePaths = false;
        boolean foundManyCardinality = query.getBase() != null && query.getBase().getFeatures().isEmpty();
        for (final Join j : query.getNavigationJoins()) {
            if (!foundManyCardinality && j instanceof ReferencedJoin && ((ReferencedJoin) j).getReference() != null && ((ReferencedJoin) j).getReference().isMany()) {
                foundManyCardinality = true;
            } else if (foundManyCardinality && (j instanceof ReferencedJoin || j instanceof ContainerJoin)) {
                multiplePaths = true;
            }
        }
        final boolean addDistinct = limit != null && multiplePaths && skipParents;

        final String sql = getSelect(addDistinct, resultContext) +
                getFrom(prefix, rdbmsBuilder.getDialect().getDualTable()) +
                getJoin(resultContext) +
                getWhere(allConditions) +
                getGroupBy(prefix) +
                getOrderBy(orderByAttributes) +
                getLimit() +
                getOffset();
        return sql;
    }

    private String getSelect(boolean addDistinct, SqlConverterContext context) {
        String columns = this.columns.stream()
                .map(c -> c.toSql(context.toBuilder().includeAlias(true).build()))
                .sorted() // sorting serves debugging purposes only
                .collect(Collectors.joining(", "));
        return "SELECT " + (addDistinct ? "DISTINCT " : "") + columns;
    }

    private String getFrom(String prefix, String dual) {
        if (from != null) {
            return "\nFROM " + from + " AS " + prefix + query.getSelect().getAlias();
        }
        if (dual != null && joins.isEmpty()) {
            return "\n FROM " + dual;
        }
        return "";
    }

    private String getJoin(SqlConverterContext context) {
        fixStaticJoins();
        final RdbmsJoin firstJoin = !joins.isEmpty() ? joins.get(0) : null;
        Map<RdbmsJoin, String> joinMap = joins.stream()
                .collect(Collectors.toMap(
                        j -> j,
                        j -> j.toSql(context, from == null && Objects.equals(j, firstJoin))));

        return joins.stream()
                    .sorted(new RdbmsJoinComparator(joins))
                    .map(j -> {
                               joinConditionTableAliases.addAll(j.getJoinConditionTableAliases());
                               return joinMap.get(j);
                           })
                    .collect(Collectors.joining());
    }

    private void fixStaticJoins() {
        List<RdbmsJoin> staticJoins =
                joins.stream()
                     .filter(j -> query.getSelect() != null && j.getPartnerTable() != null
                                  && !AsmUtils.equals(query.getSelect(), j.getPartnerTable())

                                  // current join is not a partner table to any other join in this scope
                                  && joins.stream().noneMatch(jj -> j.getPartnerTable() != null
                                                                    && jj.getAlias().equals(j.getPartnerTable().getAlias())))
                     .collect(Collectors.toList());

        for (RdbmsJoin staticJoin : staticJoins) {
            List<RdbmsJoin> dependantJoins = joins.stream().filter(j -> j.getPartnerTable() != null
                                                                        && j.getPartnerTable().getAlias().equals(staticJoin.getAlias()))
                                                  .collect(Collectors.toList());
            if (!dependantJoins.isEmpty()) {
                for (RdbmsJoin dependantJoin : dependantJoins) {
                    dependantJoin.setPartnerTable(query.getSelect());
                    dependantJoin.setPartnerColumnName(null);
                    dependantJoin.setOuter(true);
                }
                joins.remove(staticJoin);
            }
        }
    }

    private boolean isFeatureIncludedByMask(Feature f, Map<String, Object> mask, EMap<Target, Collection<String>> targetMask) {
        return (mask == null &&
                f.getTargetMappings().stream().noneMatch(tm -> tm.getTargetAttribute() != null && AsmUtils.annotatedAsTrue(tm.getTargetAttribute(), "parameterized")) ||
                f.getTargetMappings().stream().anyMatch(tm -> tm.getTarget() == null ||
                        query.getSelect().getOrderBys().stream().anyMatch(o -> AsmUtils.equals(o.getFeature(), f)) ||
                        query.getOrderBys().stream().anyMatch(o -> AsmUtils.equals(o.getFeature(), f)) ||
                        targetMask.containsKey(tm.getTarget()) && (tm.getTargetAttribute() == null ||
                                targetMask.get(tm.getTarget()).contains(tm.getTargetAttribute().getName()))
                )
        );
    }

    private boolean isFeatureId(Feature f) {
        return f instanceof IdAttribute && EcoreUtil.equals(((IdAttribute) f).getNode(), query.getSelect());
    }

    private boolean isFeatureType(Feature f) {
        return f instanceof TypeAttribute && EcoreUtil.equals(((TypeAttribute) f).getNode(), query.getSelect());
    }

    private static String getWhere(Collection<String> allConditions) {
        if (!allConditions.isEmpty()) {
            return "\nWHERE (" + String.join(") AND (", allConditions) + ")";
        }
        return "";
    }

    private String getGroupBy(String prefix) {
        if (group) {
            return "\nGROUP BY " + prefix + AGGREGATE_PREFIX + query.getAlias() + "." + getParentIdColumnAlias(query.getContainer());
        }
        return "";
    }

    private String getOrderBy(List<String> orderByAttributes) {
        if (!group && !query.getSelect().isAggregated() && !orderByAttributes.isEmpty()) {
            return "\nORDER BY " + String.join(", ", orderByAttributes);
        }
        return "";
    }

    private String getLimit() {
        if (limit != null && limit > 0) {
            return "\nLIMIT " + limit;
        }
        return "";
    }

    private String getOffset() {
        if (limit != null && limit > 0 && offset != null && offset > 0) {
            return "\nOFFSET " + offset;
        }
        return "";
    }

}
