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
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoinComparator;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsQueryJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EClass;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

public class RdbmsNavigationFilter<ID> extends RdbmsField {

    private final Filter filter;

    private final EMap<Node, EList<EClass>> ancestors = ECollections.asEMap(new HashMap<>());

    private final EMap<Node, EList<EClass>> descendants = ECollections.asEMap(new HashMap<>());

    private final String from;

    private final List<RdbmsJoin> joins = new ArrayList<>();
    private final List<RdbmsField> conditions = new ArrayList<>();

    @Builder
    private RdbmsNavigationFilter(final Filter filter, final RdbmsBuilder.RdbmsBuilderContext builderContext) {
        this.filter = filter;

        from = builderContext.rdbmsBuilder.getTableName(filter.getType());

        RdbmsBuilder.RdbmsBuilderContext forkedRdbmsContext = RdbmsBuilder.RdbmsBuilderContext.builder()
                .rdbmsBuilder(builderContext.rdbmsBuilder)
                .ancestors(ancestors)
                .descendants(descendants)
                .parentIdFilterQuery(builderContext.parentIdFilterQuery)
                .queryParameters(builderContext.queryParameters)
                .build();

        joins.addAll(filter.getJoins().stream()
                .flatMap(subJoin -> subJoin.getAllJoins().stream()
                        .flatMap(j -> (Stream<RdbmsJoin>) builderContext.rdbmsBuilder.processJoin(RdbmsBuilder.ProcessJoinParameters.builder()
                                        .join(j)
                                        .builderContext(forkedRdbmsContext)
                                        .withoutFeatures(true)
                                        .build()).stream())
                        .collect(Collectors.toList()).stream())
                .collect(Collectors.toList()));

        conditions.addAll(builderContext.rdbmsBuilder.mapFeatureToRdbms(filter.getFeature(), forkedRdbmsContext)
                .collect(Collectors.toList()));

        if (ancestors.containsKey(filter)) {
            ancestors.get(filter).forEach(ancestor ->
                    joins.addAll(builderContext.rdbmsBuilder.getAncestorJoins(filter, ancestors, joins)));
        }

        joins.addAll(filter.getSubSelects().stream()
                .filter(subSelect -> subSelect.getSelect().isAggregated())
                .map(subSelect -> {
                    final boolean group;
                    if (!subSelect.getNavigationJoins().isEmpty()) {
                        Node n = subSelect.getContainer();
                        final EList<Node> nodes = new BasicEList<>();
                        while (n != null) {
                            nodes.add(n);
                            if (n instanceof SubSelectJoin) {
                                nodes.add(((SubSelectJoin) n).getSubSelect().getBase());
                            }
                            n = (Node) n.eContainer();
                        }
                        group = subSelect.getNavigationJoins().get(0).getPartner() != null && nodes.contains(subSelect.getNavigationJoins().get(0).getPartner());
                    } else {
                        group = false;
                    }

                    final RdbmsQueryJoin<ID> queryJoin = RdbmsQueryJoin.<ID>builder()
                            .resultSet(
                                    RdbmsResultSet.<ID>builder()
                                            .query(subSelect)
                                            .filterByInstances(false)
                                            .parentIdFilterQuery(builderContext.parentIdFilterQuery)
                                            .rdbmsBuilder((RdbmsBuilder<ID>) builderContext.rdbmsBuilder)
                                            .seek(null)
                                            .withoutFeatures(true)
                                            .mask(null)
                                            .queryParameters(builderContext.queryParameters)
                                            .skipParents(false)
                                            .build()
                            )
                            .outer(true)
                            .columnName(RdbmsAliasUtil.getOptionalParentIdColumnAlias(filter))
                            .partnerTable(group ? subSelect.getBase() : null)
                            .partnerColumnName(group ? StatementExecutor.ID_COLUMN_NAME : null)
                            .alias(subSelect.getAlias())
                            .build();
                    return queryJoin;
                })
                .collect(Collectors.toList()));
    }

    @Override
    public String toSql(SqlConverterContext context) {
        final String filterPrefix = RdbmsAliasUtil.getFilterPrefix(context.prefix);

        final EMap<Node, String> newPrefixes = new BasicEMap<>();
        newPrefixes.putAll(context.prefixes);
        newPrefixes.put(filter, filterPrefix);

        final String partnerAlias;
        if (filter.eContainer() instanceof SubSelect && !(((SubSelect) filter.eContainer()).getNavigationJoins().isEmpty())) {
            partnerAlias = RdbmsAliasUtil.AGGREGATE_PREFIX + ((Node) filter.eContainer()).getAlias();
        } else if (filter.eContainer() instanceof SubSelect && (((SubSelect) filter.eContainer()).getNavigationJoins().isEmpty())) {
            partnerAlias = ((SubSelect) filter.eContainer()).getSelect().getAlias();
        } else {
            partnerAlias = ((Node) filter.eContainer()).getAlias();
        }

        checkArgument(from != null || joins.size() < 2, "Size of JOINs must be at most 1 if FROM is not set");

        final String sql = //"-- " + newPrefixes.stream().map(p -> p.getKey().getAlias() + ": " + p.getValue()).collect(Collectors.joining(", ")) + "\n" +
                "SELECT 1 " +
                (from != null ? " FROM " + from + " AS " + filterPrefix + filter.getAlias() : "") +
                getJoin(
                        context.toBuilder()
                                .prefix(filterPrefix)
                                .prefixes(newPrefixes)
                                .build()
                ) +
                "\nWHERE " + context.prefix + partnerAlias + "." + StatementExecutor.ID_COLUMN_NAME + " = " + filterPrefix + filter.getAlias() + "." + StatementExecutor.ID_COLUMN_NAME +
                joins.stream().flatMap(j -> j.conditionToSql(
                        context.toBuilder()
                                .prefix(filterPrefix)
                                .prefixes(newPrefixes)
                                .build()).stream().map(c -> " AND " + c)).collect(Collectors.joining()) +
                conditions.stream().map(c -> " AND " + c.toSql(
                        context.toBuilder()
                                .prefix(filterPrefix)
                                .includeAlias(false)
                                .prefixes(newPrefixes)
                                .build()
                )).collect(Collectors.joining());
        return sql;
    }

    private String getJoin(SqlConverterContext context) {
        Map<RdbmsJoin, String> joinMap = joins.stream().collect(Collectors.toMap(j -> j, j -> j.toSql(context, from == null)));
        return joins.stream()
                    .sorted(new RdbmsJoinComparator(joins))
                    .map(joinMap::get)
                    .collect(Collectors.joining());
    }

}
