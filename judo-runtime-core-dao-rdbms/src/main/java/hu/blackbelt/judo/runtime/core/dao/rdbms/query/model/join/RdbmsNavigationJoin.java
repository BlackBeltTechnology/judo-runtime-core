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
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor.JoinProcessParameters;
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

    private final Map<Node, List<EClass>> subAncestors = new HashMap<>();

    private final Map<Node, List<EClass>> subDescendants = new HashMap<>();

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
                                final RdbmsBuilderContext builderContext,
                                final boolean withoutFeatures) {
        super();
        final RdbmsBuilder<?> rdbmsBuilder = builderContext.getRdbmsBuilder();
        final SubSelect parentIdFilterQuery = builderContext.getParentIdFilterQuery();

        this.query = query;

        final RdbmsBuilderContext navigationBuilderContext = builderContext.toBuilder()
                .ancestors(subAncestors)
                .descendants(subDescendants)
                .build();

        outer = false;

        if (query.getPartner() != null) {
            columnName = RdbmsAliasUtil.getIdColumnAlias(query.getPartner());
            partnerTable = query.getSelect();
            partnerColumnName = StatementExecutor.ID_COLUMN_NAME;
        }
        alias = RdbmsAliasUtil.AGGREGATE_PREFIX + query.getAlias();

        final List<Join> navigationJoinList = query.getNavigationJoins();

        checkArgument(query.getBase() != null, "Base must not be null");
        checkArgument(!navigationJoinList.isEmpty(), "Navigation JOINs must not be empty");

        final EClass baseType = query.getBase() != null ? query.getBase().getType() : navigationJoinList.get(0).getType();
        subFrom = rdbmsBuilder.getTableName(baseType);

        if (query.getBase() != null &&
                !(query.getBase() instanceof Select && !(query.getContainer() instanceof SubSelectJoin) &&
                        query.getBase().getFeatures().isEmpty() && query.getSelect().isAggregated())) {
            subFeatures.add(RdbmsColumn.builder()
                    .partnerTable(query.getBase())
                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                    .alias(RdbmsAliasUtil.getParentIdColumnAlias(query.getContainer()))
                    .build());
        }

        // list of JOINs using by nested query for embedding to container query
        final List<RdbmsJoin> navigationJoins = navigationJoinList.stream()
                .flatMap(join -> rdbmsBuilder.processJoin(
                        JoinProcessParameters.builder()
                                .join(join)
                                .builderContext(navigationBuilderContext)
                                .withoutFeatures(withoutFeatures)
                                .build()
                ).stream())
                .collect(Collectors.toList());

        if (query.getBase() != null) {
            rdbmsBuilder.addAncestorJoins(subJoins, query.getBase(), navigationBuilderContext);
        }
        subJoins.addAll(navigationJoins);

        final Collection<SubSelect> aggregations = new ArrayList<>();

        final Join lastJoin = getLastJoin();

        for (Join navigationJoin : navigationJoinList) {
            final Set<String> joinedOrderByAliases = new HashSet<>();

            for (OrderBy orderBy : navigationJoin.getOrderBys()) {
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

                final List<Join> additionalJoins = new UniqueEList<>();
                additionalJoins.addAll(orderBy.getJoins().stream()
                        .flatMap(j -> j.getAllJoins().stream())
                        .collect(Collectors.toList()));

                for (Join j : additionalJoins) {
                    subJoins.addAll(rdbmsBuilder.processJoin(
                            JoinProcessParameters.builder()
                                    .join(j)
                                    .builderContext(navigationBuilderContext)
                                    .withoutFeatures(withoutFeatures)
                                    .build()
                    ));
                }

                if (orderBy.getFeature() instanceof SubSelectFeature) {
                    final SubSelect subSelect = ((SubSelectFeature) orderBy.getFeature()).getSubSelect();
                    checkArgument(subSelect.getSelect().isAggregated(),
                            "SubSelect feature must be aggregated");
                    aggregations.add(subSelect);
                }

                final List<RdbmsOrderBy> newOrderBys = rdbmsBuilder
                        .mapFeatureToRdbms(orderBy.getFeature(), navigationBuilderContext)
                        .map(o -> RdbmsOrderBy.builder()
                                .rdbmsField(o)
                                .fromSubSelect(true)
                                .descending(orderBy.isDescending())
                                .build())
                        .collect(Collectors.toList());
                // FIXME - add order by features once only
                orderBys.addAll(newOrderBys);
                if (Objects.equals(lastJoin, navigationJoin)) {
                    // only orderBys of last navigation are available for container
                    exposedOrderBys.addAll(newOrderBys);
                }
            };
        }

        subJoins.addAll(aggregations.stream()
                .map(subSelect -> RdbmsQueryJoin.<ID>builder()
                        .resultSet(
                                RdbmsResultSet.<ID>builder()
                                        .query(subSelect)
                                        .builderContext(navigationBuilderContext)
                                        .withoutFeatures(withoutFeatures)
                                        .build())
                        .outer(true)
                        .columnName(RdbmsAliasUtil.getOptionalParentIdColumnAlias(subSelect.getContainer()))
                        .partnerTable(!subSelect.getNavigationJoins().isEmpty() &&
                                Objects.equals(subSelect.getNavigationJoins().get(0).getPartner(),
                                        subSelect.getContainer()) ? subSelect.getBase() : null)
                        .partnerColumnName(!subSelect.getNavigationJoins().isEmpty() &&
                                Objects.equals(subSelect.getNavigationJoins().get(0).getPartner(),
                                        subSelect.getContainer()) ? StatementExecutor.ID_COLUMN_NAME : null)
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
                                .builderContext(navigationBuilderContext)
                                .build())
                        .build()))
                .collect(Collectors.toList()));

        if (parentIdFilterQuery != null &&
                Objects.equals(parentIdFilterQuery.getContainer(), query.getBase())) {
            subConditions.add(RdbmsFunction.builder()
                    .pattern("{0} IN (:" + RdbmsAliasUtil.getParentIdsKey(parentIdFilterQuery.getSelect()) + ")")
                    .parameter(RdbmsColumn.builder()
                            .partnerTable(query.getBase())
                            .columnName(StatementExecutor.ID_COLUMN_NAME)
                            .build())
                    .build());
        } else if (parentIdFilterQuery != null &&
                Objects.equals(parentIdFilterQuery.getSelect(), query.getBase()) &&
                !query.getSelect().isAggregated()) {
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
        if (query.getBase() != null &&
                !query.getSelect().isAggregated() &&
                query.getBase() instanceof Filter) {
            subConditions.add(RdbmsFunction.builder()
                    .pattern("{0} = {1}")
                    .parameter(RdbmsColumn.builder()
                            .partnerTable(query.getBase())
                            .columnName(StatementExecutor.ID_COLUMN_NAME)
                            .build())
                    .parameter(RdbmsColumn.builder()
                            .partnerTable(query.getBase().eContainer() instanceof SubSelect
                                    ? ((SubSelect)query.getBase().eContainer()).getSelect()
                                    : (Node) query.getBase().eContainer())
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
    protected String getTableNameOrSubQuery(SqlConverterContext converterContext) {
        final String subSelectJoinPrefix = converterContext.getPrefix() + RdbmsAliasUtil.getNavigationSubSelectAlias(query);
        aliasToCompareWith = converterContext.getPrefix() + alias;

        final Map<Node, String> newPrefixes = new HashMap<>();
        newPrefixes.putAll(converterContext.getPrefixes());
        if (query.getBase() != null) {
            newPrefixes.put(query.getBase(), subSelectJoinPrefix);
        }
        newPrefixes.putAll(query.getNavigationJoins().stream()
                .collect(Collectors.toMap(join -> join, join -> subSelectJoinPrefix)));


        SqlConverterContext subSelectSqlConverterContext = converterContext.toBuilder()
                .prefix(subSelectJoinPrefix)
                .prefixes(newPrefixes)
                .build();

        final Collection<String> allConditions = Stream
                .concat(
                        subJoins.stream().flatMap(j -> j.conditionToSql(subSelectSqlConverterContext).stream()),
                        subConditions.stream().map(c -> c.toSql(subSelectSqlConverterContext))
                ).collect(Collectors.toList());

        final String sql =
                "(" + "SELECT DISTINCT " + subFeatures.stream().map(o -> o.toSql(
                        subSelectSqlConverterContext.toBuilder()
                                .includeAlias(true)
                                .build()
                )).collect(Collectors.joining(", ")) +
                "\n    FROM " + subFrom + (query.getBase() != null ? " AS " + subSelectJoinPrefix + query.getBase().getAlias() : "") +
                getJoin(subSelectSqlConverterContext) +
                (!allConditions.isEmpty() ? "\n    WHERE " + String.join(" AND ", allConditions) : "") +
                (aggregatedNavigation ? "\nGROUP BY " + subFeatures.stream()
                        .map(f -> f.getRdbmsAlias())
                        .collect(Collectors.joining(", ")) : "") +
                ")";
        return sql;
    }

    private String getJoin(SqlConverterContext converterContext) {
        Map<RdbmsJoin, String> joinMap = subJoins.stream()
                .collect(Collectors.toMap(j -> j, j -> j.toSql(converterContext, false)));
        return subJoins.stream()
                .sorted(new RdbmsJoinComparator(subJoins))
                .map(j -> {
                    joinConditionTableAliases.addAll(j.getJoinConditionTableAliases());
                    return joinMap.get(j);
                })
                .collect(Collectors.joining());
    }

}
