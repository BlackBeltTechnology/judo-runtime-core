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
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor.FilterJoinProcessor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor.FilterJoinProcessorParameters;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import lombok.Builder;
import lombok.NonNull;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EClass;

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
            final RdbmsBuilderContext builderContext) {

        RdbmsBuilderContext countBuilderContext = builderContext.toBuilder()
                .ancestors(ancestors)
                .descendants(descendants)
                .build();

        final EClass type = query.getSelect().getType();

        this.query = query;
        this.rdbmsBuilder = (RdbmsBuilder<ID>) builderContext.getRdbmsBuilder();
        this.from = type != null ? rdbmsBuilder.getTableName(type) : (String) null;


        rdbmsBuilder.addAncestorJoins(joins, query.getSelect(), ancestors, countBuilderContext);
        // joins.addAll(rdbmsBuilder.getDescendantJoins(query.getSelect(), descendants, joins));

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
                            .builderContext(countBuilderContext)
                            .withoutFeatures(true)
                            .build();
            joins.add(customJoin);
        }

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
                            .build(), countBuilderContext);
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
                            .build(), countBuilderContext);
        }
    }

    public String toSql(SqlConverterContext context) {
        final String prefix = context.prefix;
        final EMap<Node, String> prefixes = context.prefixes;

        final EMap<Node, String> newPrefixes = new BasicEMap<>();
        newPrefixes.putAll(prefixes);
        newPrefixes.put(query.getSelect(), prefix);
        newPrefixes.putAll(query.getSelect().getAllJoins().stream()
                .collect(Collectors.toMap(join -> join, join -> prefix)));

        final RdbmsJoin firstJoin = !joins.isEmpty() ? joins.get(0) : null;

        SqlConverterContext countContext = context.toBuilder()
                .prefixes(newPrefixes)
                .build();

        final Collection<String> allConditions = Stream
                .concat(
                        joins.stream().flatMap(j -> j.conditionToSql(countContext).stream()),
                        conditions.stream().map(c -> c.toSql(countContext))
                )
                .collect(Collectors.toList());

        final String dual = rdbmsBuilder.getDialect().getDualTable();
        final String sql = //"-- " + newPrefixes.stream().map(p -> p.getKey().getAlias() + ": " + p.getValue()).collect(Collectors.joining(", ")) + "\n" +
                "SELECT COUNT (1)" +
                (from != null ? "\nFROM " + from + " AS " + prefix + query.getSelect().getAlias() : (dual != null && joins.isEmpty() ? "\n FROM " + dual : "")) +
                getJoin(countContext, firstJoin) +
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
