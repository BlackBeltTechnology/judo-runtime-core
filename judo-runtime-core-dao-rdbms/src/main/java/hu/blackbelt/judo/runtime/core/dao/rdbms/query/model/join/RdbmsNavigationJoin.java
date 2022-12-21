package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join;

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
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import hu.blackbelt.mapper.api.Coercer;
import lombok.*;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EClass;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

public class RdbmsNavigationJoin<ID> extends RdbmsJoin {

    private final EMap<Node, EList<EClass>> subAncestors = ECollections.asEMap(new HashMap<>());
    private final String subFrom;
    private final List<RdbmsJoin> subJoins = new ArrayList<>();
    private final List<RdbmsField> subFeatures = new ArrayList<>();
    private final List<RdbmsField> subConditions = new ArrayList<>();

    private final List<RdbmsOrderBy> orderBys = new ArrayList<>();

    @Getter
    private final List<RdbmsOrderBy> exposedOrderBys = new ArrayList<>();

    final boolean aggregatedNavigation;

    private final SubSelect query;

    @Builder
    private RdbmsNavigationJoin(@NonNull final SubSelect query,
                                final SubSelect parentIdFilterQuery,
                                final RdbmsBuilder<ID> rdbmsBuilder,
                                final boolean withoutFeatures,
                                final Map<String, Object> queryParameters) {
        super();
        this.query = query;

        outer = false;

        if (query.getPartner() != null) {
            columnName = RdbmsAliasUtil.getIdColumnAlias(query.getPartner());
            partnerTable = query.getSelect();
            partnerColumnName = StatementExecutor.ID_COLUMN_NAME;
        }
        alias = RdbmsAliasUtil.AGGREGATE_PREFIX + query.getAlias();

        final EList<Join> navigationJoinList = query.getNavigationJoins();

        checkArgument(query.getBase() != null, "Base must not be null");
        checkArgument(!navigationJoinList.isEmpty(), "Navigation JOINs must not be empty");

        final EClass baseType = query.getBase() != null ? query.getBase().getType() : navigationJoinList.get(0).getType();
        subFrom = rdbmsBuilder.getTableName(baseType);

        if (query.getBase() != null && !(query.getBase() instanceof Select && !(query.getContainer() instanceof SubSelectJoin) && query.getBase().getFeatures().isEmpty() && query.getSelect().isAggregated())) {
            subFeatures.add(RdbmsColumn.builder()
                    .partnerTable(query.getBase())
                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                    .alias(RdbmsAliasUtil.getParentIdColumnAlias(query.getContainer()))
                    .build());
        }

        // list of JOINs using by nested query for embedding to container query
        final List<RdbmsJoin> navigationJoins = navigationJoinList.stream()
                .flatMap(join -> rdbmsBuilder.processJoin(join, subAncestors, parentIdFilterQuery, rdbmsBuilder, withoutFeatures, null, queryParameters).stream())
                .collect(Collectors.toList());

        if (query.getBase() != null) {
            subJoins.addAll(rdbmsBuilder.getAdditionalJoins(query.getBase(), subAncestors, subJoins));
        }
        subJoins.addAll(navigationJoins);

        final Collection<SubSelect> aggregations = new ArrayList<>();

        final Join lastJoin = getLastJoin();
        navigationJoinList.forEach(navigationJoin -> {
            final Set<String> joinedOrderByAliases = new HashSet<>();

            navigationJoin.getOrderBys().forEach(orderBy -> {
                if (!joinedOrderByAliases.contains(orderBy.getAlias())) {
                    subJoins.add(RdbmsTableJoin.builder()
                            .tableName(rdbmsBuilder.getTableName(orderBy.getType()))
                            .columnName(StatementExecutor.ID_COLUMN_NAME)
                            .partnerTable(navigationJoin)
                            .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                            .alias(orderBy.getAlias())
                            .build());
                    joinedOrderByAliases.add(orderBy.getAlias());
                }

                final EList<Join> additionalJoins = new UniqueEList<>();
                additionalJoins.addAll(orderBy.getJoins().stream().flatMap(j -> j.getAllJoins().stream()).collect(Collectors.toList()));
                additionalJoins.forEach(j -> subJoins.addAll(rdbmsBuilder.processJoin(j, subAncestors, parentIdFilterQuery, rdbmsBuilder, withoutFeatures, null, queryParameters)));

                if (orderBy.getFeature() instanceof SubSelectFeature) {
                    final SubSelect subSelect = ((SubSelectFeature) orderBy.getFeature()).getSubSelect();
                    checkArgument(subSelect.getSelect().isAggregated(), "SubSelect feature must be aggregated");

                    aggregations.add(subSelect);
                }

                final List<RdbmsOrderBy> newOrderBys = rdbmsBuilder.mapFeatureToRdbms(orderBy.getFeature(), subAncestors, parentIdFilterQuery, queryParameters)
                        .map(o -> RdbmsOrderBy.builder()
                                .rdbmsField(o)
                                .fromSubSelect(true)
                                .descending(orderBy.isDescending())
                                .build())
                        .collect(Collectors.toList());
                // FIXME - add order by features once only
                orderBys.addAll(newOrderBys);
                if (AsmUtils.equals(lastJoin, navigationJoin)) {
                    // only orderBys of last navigation are available for container
                    exposedOrderBys.addAll(newOrderBys);
                }
            });
        });

        subJoins.addAll(aggregations.stream()
                .map(subSelect -> RdbmsQueryJoin.<ID>builder()
                        .resultSet(
                                RdbmsResultSet.<ID>builder()
                                        .query(subSelect)
                                        .filterByInstances(false)
                                        .parentIdFilterQuery(parentIdFilterQuery)
                                        .rdbmsBuilder(rdbmsBuilder)
                                        .seek(null)
                                        .withoutFeatures(withoutFeatures)
                                        .mask(null)
                                        .queryParameters(queryParameters)
                                        .skipParents(false)
                                        .build())
                        .outer(true)
                        .columnName(RdbmsAliasUtil.getOptionalParentIdColumnAlias(subSelect.getContainer()))
                        .partnerTable(!subSelect.getNavigationJoins().isEmpty() && AsmUtils.equals(subSelect.getNavigationJoins().get(0).getPartner(), subSelect.getContainer()) ? subSelect.getBase() : null)
                        .partnerColumnName(!subSelect.getNavigationJoins().isEmpty() && AsmUtils.equals(subSelect.getNavigationJoins().get(0).getPartner(), subSelect.getContainer()) ? StatementExecutor.ID_COLUMN_NAME : null)
                        .alias(subSelect.getAlias())
                        .build())
                .collect(Collectors.toList()));

        // TODO - add subFilterFeatures
        subFeatures.addAll(orderBys.stream().map(o -> o.getRdbmsField()).collect(Collectors.toList()));

        if (query.getPartner() != null) {
            subFeatures.add(RdbmsColumn.builder()
                    .partnerTable(query.getPartner())
                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                    .alias(RdbmsAliasUtil.getIdColumnAlias(query.getPartner()))
                    .build());

            subConditions.add(RdbmsFunction.builder()
                    .pattern("{0} IS NOT NULL")
                    .parameter(RdbmsColumn.builder()
                            .partnerTable(query.getPartner())
                            .columnName(StatementExecutor.ID_COLUMN_NAME)
                            .build())
                    .build());
        }

        subConditions.addAll(query.getNavigationJoins().stream()
                .flatMap(join -> join.getFilters().stream().map(f -> RdbmsFunction.builder()
                        .pattern("EXISTS ({0})")
                        .parameter(RdbmsNavigationFilter.<ID>builder()
                                .filter(f)
                                .rdbmsBuilder(rdbmsBuilder)
                                .parentIdFilterQuery(parentIdFilterQuery)
                                .queryParameters(queryParameters)
                                .build())
                        .build()))
                .collect(Collectors.toList()));

        if (parentIdFilterQuery != null && AsmUtils.equals(parentIdFilterQuery.getContainer(), query.getBase())) {
            subConditions.add(RdbmsFunction.builder()
                    .pattern("{0} IN (:" + RdbmsAliasUtil.getParentIdsKey(parentIdFilterQuery.getSelect()) + ")")
                    .parameter(RdbmsColumn.builder()
                            .partnerTable(query.getBase())
                            .columnName(StatementExecutor.ID_COLUMN_NAME)
                            .build())
                    .build());
        } else if (parentIdFilterQuery != null && AsmUtils.equals(parentIdFilterQuery.getSelect(), query.getBase()) && !query.getSelect().isAggregated()) {
            subConditions.add(RdbmsFunction.builder()
                    .pattern("{0} = {1}")
                    .parameter(RdbmsColumn.builder()
                            .partnerTable(query.getBase())
                            .columnName(StatementExecutor.ID_COLUMN_NAME)
                            .build())
                    .parameter(RdbmsColumn.builder()
                            .partnerTable(parentIdFilterQuery.getSelect())
                            .columnName(StatementExecutor.ID_COLUMN_NAME)
                            .skipLastPrefix(true)
                            .build())
                    .build());
        }
        if (query.getBase() != null && !query.getSelect().isAggregated() && query.getBase() instanceof Filter) {
            subConditions.add(RdbmsFunction.builder()
                    .pattern("{0} = {1}")
                    .parameter(RdbmsColumn.builder()
                            .partnerTable(query.getBase())
                            .columnName(StatementExecutor.ID_COLUMN_NAME)
                            .build())
                    .parameter(RdbmsColumn.builder()
                            .partnerTable(query.getBase().eContainer() instanceof SubSelect ? ((SubSelect)query.getBase().eContainer()).getSelect() : (Node) query.getBase().eContainer())
                            .columnName(StatementExecutor.ID_COLUMN_NAME)
                            .build())
                    .build());
        }

        aggregatedNavigation = !aggregations.isEmpty();
    }

    public Join getLastJoin() {
        Join j = query.getJoins().get(query.getJoins().size() - 1);
        while (!j.getJoins().isEmpty()) {
            j = j.getJoins().get(j.getJoins().size() - 1);
        }
        return j;
    }

    @Override
    protected String getTableNameOrSubQuery(final String prefix, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        final String subSelectJoinPrefix = prefix + RdbmsAliasUtil.getNavigationSubSelectAlias(query);
        aliasToCompareWith = prefix + alias;

        final EMap<Node, String> newPrefixes = new BasicEMap<>();
        newPrefixes.putAll(prefixes);
        if (query.getBase() != null) {
            newPrefixes.put(query.getBase(), subSelectJoinPrefix);
        }
        newPrefixes.putAll(query.getNavigationJoins().stream()
                .collect(Collectors.toMap(join -> join, join -> subSelectJoinPrefix)));

        final Collection<String> allConditions = Stream
                .concat(subJoins.stream().flatMap(j -> j.conditionToSql(subSelectJoinPrefix, coercer, sqlParameters, newPrefixes).stream()),
                        subConditions.stream().map(c -> c.toSql(subSelectJoinPrefix, false, coercer, sqlParameters, newPrefixes)))
                .collect(Collectors.toList());
        final String sql = //"-- " + newPrefixes.stream().map(p -> p.getKey().getAlias() + ": " + p.getValue()).collect(Collectors.joining(", ")) + "\n" +
                "(" + "SELECT DISTINCT " + subFeatures.stream().map(o -> o.toSql(subSelectJoinPrefix, true, coercer, sqlParameters, newPrefixes)).collect(Collectors.joining(", ")) +
                "\n    FROM " + subFrom + (query.getBase() != null ? " AS " + subSelectJoinPrefix + query.getBase().getAlias() : "") +
                getJoin(coercer, sqlParameters, subSelectJoinPrefix, newPrefixes) +
                (!allConditions.isEmpty() ? "\n    WHERE " + String.join(" AND ", allConditions) : "") +
                (aggregatedNavigation ? "\nGROUP BY " + subFeatures.stream().map(f -> f.getRdbmsAlias()).collect(Collectors.joining(", ")) : "") +
                ")";
        return sql;
    }

    private String getJoin(Coercer coercer, MapSqlParameterSource sqlParameters, String subSelectJoinPrefix, EMap<Node, String> newPrefixes) {
        Map<RdbmsJoin, String> joinMap = subJoins.stream().collect(Collectors.toMap(j -> j, j -> j.toSql(subSelectJoinPrefix, coercer, sqlParameters, newPrefixes, false)));
        return subJoins.stream()
                       .sorted(new RdbmsJoinComparator(subJoins))
                       .map(j -> {
                           joinConditionTableAliases.addAll(j.getJoinConditionTableAliases());
                           return joinMap.get(j);
                       })
                       .collect(Collectors.joining());
    }

}
