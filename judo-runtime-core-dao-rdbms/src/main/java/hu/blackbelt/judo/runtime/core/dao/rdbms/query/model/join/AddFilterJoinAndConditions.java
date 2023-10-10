package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsResultSet;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import lombok.Builder;
import lombok.NonNull;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.UniqueEList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil.getParentIdColumnAlias;

public class AddFilterJoinAndConditions {

    @Builder
    public static class AddFilterJoinsAndConditionsParameters {
        @NonNull
        List<RdbmsJoin> joins;
        @NonNull
        SubSelect query;
        @NonNull
        EList<Join> processedNodesForJoins;
        @NonNull
        List<RdbmsField> conditions;
        @Builder.Default
        String partnerTablePrefix = "";
        @NonNull
        Filter filter;
        @NonNull
        Node partnerTable;
        boolean addJoinsOfFilterFeature;
        @NonNull
        RdbmsBuilder.RdbmsBuilderContext builderContext;
    }
    public static <ID> void addFilterJoinsAndConditions(AddFilterJoinsAndConditionsParameters params) {
        if (!params.joins.stream().anyMatch(j -> Objects.equals(params.filter.getAlias(), j.getAlias()))) {
            params.joins.add(RdbmsTableJoin.builder()
                    .tableName(params.builderContext.rdbmsBuilder.getTableName(params.filter.getType()))
                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                    .partnerTablePrefix(params.partnerTablePrefix)
                    .partnerTable(params.partnerTable)
                    .partnerColumnName(params.partnerTable instanceof SubSelect ? getParentIdColumnAlias(params.query.getContainer()) : StatementExecutor.ID_COLUMN_NAME)
                    .alias(params.filter.getAlias())
                    .build());
            if (params.addJoinsOfFilterFeature) {
                final EList<Join> navigationJoins = new UniqueEList<>();
                final EList<Join> targetJoins = new UniqueEList<>(params.filter.getFeature().getNodes().stream()
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
                        .filter(join -> !params.processedNodesForJoins.contains(join))
                        .forEach(join -> {
                            params.joins.addAll(params.builderContext.rdbmsBuilder.processJoin(
                                    RdbmsBuilder.ProcessJoinParameters.builder()
                                            .builderContext(params.builderContext)
                                            .join(join)
                                            .withoutFeatures(false)
                                            .build()
                            ));
                            params.processedNodesForJoins.add(join);
                        });
                params.filter.getFeature().getNodes().stream()
                        .filter(n -> !params.processedNodesForJoins.contains(n) && !AsmUtils.equals(n, params.filter) && n instanceof Join)
                        .flatMap(n -> ((Join) n).getAllJoins().stream())
                        .forEach(join -> {
                            params.joins.addAll(params.builderContext.rdbmsBuilder.processJoin(
                                    RdbmsBuilder.ProcessJoinParameters.builder()
                                            .builderContext(params.builderContext)
                                            .join(join)
                                            .withoutFeatures(false)
                                            .build()
                            ));
                            params.processedNodesForJoins.add(join);
                        });
            }
        }

        params.joins.addAll(params.filter.getFeatures().stream()
                .filter(f -> f instanceof SubSelectFeature).map(f -> (SubSelectFeature) f)
                .filter(f -> !params.joins.stream().anyMatch(j -> Objects.equals(f.getSubSelect().getAlias(), j.getAlias())))
                .map(f -> RdbmsQueryJoin.<ID>builder()
                        .resultSet(
                                RdbmsResultSet.<ID>builder()
                                        .query(f.getSubSelect())
                                        .filterByInstances(false)
                                        .parentIdFilterQuery(params.builderContext.parentIdFilterQuery)
                                        .rdbmsBuilder((RdbmsBuilder<ID>) params.builderContext.rdbmsBuilder)
                                        .withoutFeatures(true)
                                        .queryParameters(params.builderContext.queryParameters)
                                        .skipParents(false)
                                        .build())
                        .outer(true)
                        .columnName(RdbmsAliasUtil.getOptionalParentIdColumnAlias(f.getSubSelect().getContainer()))
                        .partnerTable(f.getSubSelect().getNavigationJoins().isEmpty() ? null : f.getSubSelect().getContainer())
                        .partnerColumnName(f.getSubSelect().getNavigationJoins().isEmpty() ? null : StatementExecutor.ID_COLUMN_NAME)
                        .alias(f.getSubSelect().getAlias())
                        .build())
                .collect(Collectors.toList()));
        params.conditions.addAll(params.builderContext.rdbmsBuilder.mapFeatureToRdbms(params.filter.getFeature(), params.builderContext).collect(Collectors.toList()));
        params.joins.addAll(params.builderContext.rdbmsBuilder.getAncestorJoins(params.filter, params.builderContext.ancestors, params.joins));
    }

}
