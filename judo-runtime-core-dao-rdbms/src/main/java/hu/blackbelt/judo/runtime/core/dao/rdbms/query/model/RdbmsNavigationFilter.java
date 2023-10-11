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

        RdbmsBuilder<ID> rdbmsBuilder = (RdbmsBuilder<ID>) builderContext.rdbmsBuilder;

        RdbmsBuilder.RdbmsBuilderContext navigationBuilderContext = builderContext.toBuilder()
                .ancestors(ancestors)
                .descendants(descendants)
                .build();

        joins.addAll(filter.getJoins().stream()
                .flatMap(subJoin -> subJoin.getAllJoins().stream()
                        .flatMap(j -> (Stream<RdbmsJoin>) rdbmsBuilder.processJoin(RdbmsBuilder.ProcessJoinParameters.builder()
                                        .join(j)
                                        .builderContext(navigationBuilderContext)
                                        .withoutFeatures(true)
                                        .mask(null)
                                        .build()).stream())
                        .collect(Collectors.toList()).stream())
                .collect(Collectors.toList()));

        conditions.addAll(rdbmsBuilder.mapFeatureToRdbms(filter.getFeature(), navigationBuilderContext)
                .collect(Collectors.toList()));

        if (ancestors.containsKey(filter)) {
            rdbmsBuilder.addAncestorJoins(joins, filter, ancestors);
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
                                            .rdbmsBuilder((RdbmsBuilder<ID>) rdbmsBuilder)
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

        final SqlConverterContext navigationContext = context.toBuilder()
                .prefix(filterPrefix)
                .prefixes(newPrefixes)
                .build();

        return "SELECT 1 " + getFrom(navigationContext) + getWhere(context.prefix, partnerAlias, navigationContext);
    }

    private String getFrom(SqlConverterContext context) {
        return (from != null ? " FROM " + from + " AS " + context.prefix + filter.getAlias() : "") + getJoin(context);
    }

    private String getWhere(String partnerPrefix, String partnerAlias, SqlConverterContext context) {
        return "\nWHERE " +
                partnerPrefix + partnerAlias + "." + StatementExecutor.ID_COLUMN_NAME + " = " +
                    context.prefix + filter.getAlias() + "." + StatementExecutor.ID_COLUMN_NAME +
                joins.stream()
                        .flatMap(j -> j.conditionToSql(context).stream().map(c -> " AND " + c))
                        .collect(Collectors.joining()) +
                conditions.stream()
                        .map(c -> " AND " + c.toSql(context.toBuilder().includeAlias(false).build()))
                        .collect(Collectors.joining());
    }

    private String getJoin(SqlConverterContext context) {
        Map<RdbmsJoin, String> joinMap = joins.stream().collect(Collectors.toMap(j -> j, j -> j.toSql(context, from == null)));

        return joins.stream()
                    .sorted(new RdbmsJoinComparator(joins))
                    .map(joinMap::get)
                    .collect(Collectors.joining());
    }

}
