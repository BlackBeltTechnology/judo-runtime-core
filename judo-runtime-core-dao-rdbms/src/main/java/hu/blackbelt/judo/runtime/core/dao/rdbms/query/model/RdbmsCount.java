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

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EClass;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil.AGGREGATE_PREFIX;

public class RdbmsCount<ID> {

    private SubSelect query;

    // map of ancestors (the are holder of an attribute) of a given source
    private final EMap<Node, EList<EClass>> ancestors = ECollections.asEMap(new HashMap<>());

    private final EMap<Node, EList<EClass>> descendants = ECollections.asEMap(new HashMap<>());

    private final String from;
    private final List<RdbmsJoin> joins = new ArrayList<>();
    private final List<RdbmsField> conditions = new ArrayList<>();

    private final EList<Join> processedNodesForJoins = ECollections.newBasicEList();

    private final RdbmsBuilder<ID> rdbmsBuilder;

    @Builder
    private RdbmsCount(
            @NonNull final SubSelect query,
            final boolean filterByInstances,
            final SubSelect parentIdFilterQuery,
            final RdbmsBuilder<ID> rdbmsBuilder,
            final Map<String, Object> queryParameters) {

        RdbmsBuilder.RdbmsBuilderContext context = RdbmsBuilder.RdbmsBuilderContext.builder()
                .rdbmsBuilder(rdbmsBuilder)
                .ancestors(ancestors)
                .descendants(descendants)
                .parentIdFilterQuery(parentIdFilterQuery)
                .queryParameters(queryParameters)
                .build();

        this.query = query;
        this.rdbmsBuilder = rdbmsBuilder;

        final EClass type = query.getSelect().getType();
        from = type != null ? rdbmsBuilder.getTableName(type) : null;

        joins.addAll(rdbmsBuilder.getAncestorJoins(query.getSelect(), ancestors, joins));
        joins.addAll(rdbmsBuilder.getDescendantJoins(query.getSelect(), descendants, joins));

        if (filterByInstances) {
            conditions.add(RdbmsFunction.builder()
                    .pattern("{0} IN ({1})")
                    .parameters(Arrays.asList(
                            RdbmsColumn.builder().partnerTable(query.getSelect()).columnName(StatementExecutor.ID_COLUMN_NAME).build(),
                            RdbmsParameter.builder().parameterName(RdbmsAliasUtil.getInstanceIdsKey(query.getSelect())).build()
                    )).build());
        }

        final boolean addJoinOfFilterFeature =
                (query.getBase() != null &&
                        !(query.getBase() instanceof Select &&
                                !(query.getContainer() instanceof SubSelectJoin) &&
                                query.getBase().getFeatures().isEmpty() && query.getSelect().isAggregated())) ||
                        (query.getPartner() == null && query.eContainer() == null);

        if (!query.getNavigationJoins().isEmpty()) {
            final RdbmsNavigationJoin<ID> customJoin =
                    RdbmsNavigationJoin.<ID>builder()
                            .query(query)
                            .parentIdFilterQuery(parentIdFilterQuery)
                            .rdbmsBuilder(rdbmsBuilder)
                            .withoutFeatures(true)
                            .queryParameters(queryParameters)
                            .build();
            joins.add(customJoin);
        }

        query.getFilters().forEach(filter ->
                AddFilterJoinAndConditions.addFilterJoinsAndConditions(
                        AddFilterJoinAndConditions.AddFilterJoinsAndConditionsParameters.builder()
                                .builderContext(context)
                                .joins(joins)
                                .conditions(conditions)
                                .processedNodesForJoins(processedNodesForJoins)
                                .query(query)
                                .filter(filter)
                                .partnerTable(query.getPartner() != null ? query : query.getSelect())
                                .partnerTablePrefix(query.getPartner() != null ? AGGREGATE_PREFIX : "")
                                .addJoinsOfFilterFeature(addJoinOfFilterFeature)
                                .build()));

        query.getSelect().getFilters().forEach(filter ->
                AddFilterJoinAndConditions.addFilterJoinsAndConditions(
                        AddFilterJoinAndConditions.AddFilterJoinsAndConditionsParameters.builder()
                                .builderContext(context)
                                .joins(joins)
                                .conditions(conditions)
                                .processedNodesForJoins(processedNodesForJoins)
                                .query(query)
                                .filter(filter)
                                .partnerTable(query.getSelect())
                                .addJoinsOfFilterFeature(true)
                                .build()));

//        query.getFilters().forEach(filter ->
//                addFilterJoinsAndConditions(filter, rdbmsBuilder, parentIdFilterQuery, query.getPartner() != null ? query : query.getSelect(), query.getPartner() != null ? RdbmsAliasUtil.AGGREGATE_PREFIX : "", addJoinOfFilterFeature, queryParameters));

//        query.getSelect().getFilters().forEach(filter ->
//                addFilterJoinsAndConditions(filter, rdbmsBuilder, parentIdFilterQuery, query.getSelect(), "", true, queryParameters));

    }

    /*
    private void addFilterJoinsAndConditions(final Filter filter, final RdbmsBuilder<ID> rdbmsBuilder, final SubSelect parentIdFilterQuery, final Node partnerTable, final String partnerTablePrefix, final boolean addJoinsOfFilterFeature, final Map<String, Object> queryParameters) {
        RdbmsBuilder.RdbmsBuilderContext context = RdbmsBuilder.RdbmsBuilderContext.builder()
                .ancestors(ancestors)
                .descendants(descendants)
                .parentIdFilterQuery(parentIdFilterQuery)
                .queryParameters(queryParameters)
                .build();


        if (!joins.stream().anyMatch(j -> Objects.equals(filter.getAlias(), j.getAlias()))) {
            joins.add(RdbmsTableJoin.builder()
                    .tableName(rdbmsBuilder.getTableName(filter.getType()))
                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                    .partnerTablePrefix(partnerTablePrefix)
                    .partnerTable(partnerTable)
                    .partnerColumnName(partnerTable instanceof SubSelect ? RdbmsAliasUtil.getParentIdColumnAlias(query.getContainer()) : StatementExecutor.ID_COLUMN_NAME)
                    .alias(filter.getAlias())
                    .build());
            if (addJoinsOfFilterFeature) {
                final EList<Join> navigationJoins = new UniqueEList<>();
                final EList<Join> targetJoins = new UniqueEList<>(filter.getFeature().getNodes().stream()
                        .filter(n -> n instanceof Join)
                        .map(n -> (Join) n)
                        .collect(Collectors.toList()));
                while (!targetJoins.isEmpty()) {
                    final List<Join> newNavigationJoins = targetJoins.stream()
                            .map(j -> j.getPartner())
                            .filter(n -> n instanceof Join && !navigationJoins.contains(n))
                            .map(n -> (Join) n)
                            .collect(Collectors.toList());
                    navigationJoins.addAll(newNavigationJoins);
                    targetJoins.clear();
                    targetJoins.addAll(newNavigationJoins);
                }

                ECollections.reverse(navigationJoins);
                navigationJoins.stream()
                        .filter(join -> !processedNodesForJoins.contains(join))
                        .forEach(join -> {
                            joins.addAll(rdbmsBuilder.processJoin(
                                    RdbmsBuilder.ProcessJoinParameters.builder()
                                            .builderContext(context)
                                            .join(join)
                                            .withoutFeatures(false)
                                            .build()));
                            processedNodesForJoins.add(join);
                        });
                filter.getFeature().getNodes().stream()
                        .filter(n -> !processedNodesForJoins.contains(n) && !AsmUtils.equals(n, filter) && n instanceof Join)
                        .flatMap(n -> ((Join) n).getAllJoins().stream())
                        .forEach(join -> {
                            joins.addAll(rdbmsBuilder.processJoin(
                                    RdbmsBuilder.ProcessJoinParameters.builder()
                                            .builderContext(context)
                                            .join(join)
                                            .withoutFeatures(false)
                                            .build()));
                            processedNodesForJoins.add(join);
                        });
            }
        }

        joins.addAll(filter.getFeatures().stream()
                .filter(f -> f instanceof SubSelectFeature).map(f -> (SubSelectFeature) f)
                .filter(f -> !joins.stream().anyMatch(j -> Objects.equals(f.getSubSelect().getAlias(), j.getAlias())))
                .map(f -> RdbmsQueryJoin.<ID>builder()
                        .resultSet(
                                RdbmsResultSet.<ID>builder()
                                        .query(f.getSubSelect())
                                        .filterByInstances(false)
                                        .parentIdFilterQuery(parentIdFilterQuery)
                                        .rdbmsBuilder(rdbmsBuilder)
                                        .seek(null)
                                        .withoutFeatures(true)
                                        .mask(null)
                                        .queryParameters(queryParameters)
                                        .skipParents(false)
                                        .build())
                        .outer(true)
                        .columnName(RdbmsAliasUtil.getOptionalParentIdColumnAlias(f.getSubSelect().getContainer()))
                        .partnerTable(f.getSubSelect().getNavigationJoins().isEmpty() ? null : f.getSubSelect().getContainer())
                        .partnerColumnName(f.getSubSelect().getNavigationJoins().isEmpty() ? null : StatementExecutor.ID_COLUMN_NAME)
                        .alias(f.getSubSelect().getAlias())
                        .build())
                .collect(Collectors.toList()));
        conditions.addAll(rdbmsBuilder.mapFeatureToRdbms(filter.getFeature(), context).collect(Collectors.toList()));
        joins.addAll(rdbmsBuilder.getAncestorJoins(filter, ancestors, joins));
    }
    */

    public String toSql(SqlConverterContext params) {

        final EMap<Node, String> newPrefixes = new BasicEMap<>();
        newPrefixes.putAll(params.prefixes);
        newPrefixes.put(query.getSelect(), params.prefix);
        newPrefixes.putAll(query.getSelect().getAllJoins().stream()
                .collect(Collectors.toMap(join -> join, join -> params.prefix)));

        final RdbmsJoin firstJoin = !joins.isEmpty() ? joins.get(0) : null;

        final Collection<String> allConditions = Stream
                .concat(joins.stream().flatMap(j -> j.conditionToSql(
                        params.toBuilder()
                                .prefixes(newPrefixes)
                                .build())
                                .stream()),
                        conditions.stream().map(c -> c.toSql(
                                params.toBuilder()
                                        .includeAlias(false)
                                        .prefixes(newPrefixes)
                                        .build())
                        ))
                .collect(Collectors.toList());

        final String dual = rdbmsBuilder.getDialect().getDualTable();
        final String sql = //"-- " + newPrefixes.stream().map(p -> p.getKey().getAlias() + ": " + p.getValue()).collect(Collectors.joining(", ")) + "\n" +
                "SELECT COUNT (1)" +
                (from != null ? "\nFROM " + from + " AS " + params.prefix + query.getSelect().getAlias() : (dual != null && joins.isEmpty() ? "\n FROM " + dual : "")) +
                getJoin(
                        params.toBuilder()
                                .prefixes(newPrefixes)
                                .build(), firstJoin
                ) +
                (!allConditions.isEmpty() ? "\nWHERE (" + String.join(") AND (", allConditions) + ")" : "");

        return sql;
    }

    private String getJoin(SqlConverterContext context, RdbmsJoin firstJoin) {
        Map<RdbmsJoin, String> joinMap = joins.stream().collect(Collectors.toMap(j -> j, j -> j.toSql(context, from == null && Objects.equals(j, firstJoin))));
        return joins.stream()
                    .sorted(new RdbmsJoinComparator(joins))
                    .map(joinMap::get)
                    .collect(Collectors.joining());
    }

}
