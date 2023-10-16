package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsResultSet;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsQueryJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsTableJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor.JoinProcessParameters;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import lombok.Builder;
import lombok.NonNull;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.UniqueEList;
import org.eclipse.emf.ecore.EClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil.getParentIdColumnAlias;

@Builder
public class FilterJoinProcessor {

    public <ID> void addFilterJoinsAndConditions(final FilterJoinProcessorParameters params, final RdbmsBuilderContext builderContext) {
        final RdbmsBuilder<ID> rdbmsBuilder = (RdbmsBuilder<ID>) builderContext.getRdbmsBuilder();
        final List<RdbmsJoin> joins = params.getJoins();
        final Filter filter = params.getFilter();
        final SubSelect query = params.getQuery();
        final EList<Join> processedNodesForJoins = params.getProcessedNodesForJoins();
        final List<RdbmsField> conditions = params.getConditions();
        final String partnerTablePrefix = params.getPartnerTablePrefix();
        final Node partnerTable = params.getPartnerTable();
        final Boolean addJoinsOfFilterFeature = params.isAddJoinsOfFilterFeature();
        final EMap<Node, EList<EClass>> ancestors = builderContext.getAncestors();
        final EMap<Node, EList<EClass>> descendants = builderContext.getDescendants();
        final SubSelect parentIdFilterQuery = builderContext.getParentIdFilterQuery();
        final Map<String, Object> queryParameters = builderContext.getQueryParameters();

        if (!joins.stream().anyMatch(j -> Objects.equals(filter.getAlias(), j.getAlias()))) {
            joins.add(RdbmsTableJoin.builder()
                    .tableName(rdbmsBuilder.getTableName(filter.getType()))
                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                    .partnerTablePrefix(partnerTablePrefix)
                    .partnerTable(partnerTable)
                    .partnerColumnName(partnerTable instanceof SubSelect ? getParentIdColumnAlias(query.getContainer()) : StatementExecutor.ID_COLUMN_NAME)
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
                List<Join> navigationJoinsNotProcessed =  navigationJoins.stream()
                        .filter(join -> !processedNodesForJoins.contains(join)).collect(Collectors.toList());

                for (Join join : navigationJoinsNotProcessed) {
                    joins.addAll(rdbmsBuilder.processJoin(
                            JoinProcessParameters.builder()
                                    .builderContext(builderContext)
                                    .join(join)
                                    .withoutFeatures(false)
                                    .mask(null)
                                    .build()
                    ));
                    processedNodesForJoins.add(join);
                };

                List<Join> filterFeaturesNoProcessed = filter.getFeature().getNodes().stream()
                        .filter(n -> !processedNodesForJoins.contains(n) && !AsmUtils.equals(n, filter) && n instanceof Join)
                        .flatMap(n -> ((Join) n).getAllJoins().stream())
                        .collect(Collectors.toList());

                for (Join join : filterFeaturesNoProcessed) {
                    joins.addAll(rdbmsBuilder.processJoin(
                            JoinProcessParameters.builder()
                                    .builderContext(builderContext)
                                    .join(join)
                                    .withoutFeatures(false)
                                    .mask(null)
                                    .build()
                    ));
                    processedNodesForJoins.add(join);
                };
            }
        }

        List<SubSelectFeature> subSelectFilterFeaturesNotProcessed = filter.getFeatures().stream()
                        .filter(f -> f instanceof SubSelectFeature).map(f -> (SubSelectFeature) f)
                        .filter(f -> !joins.stream().anyMatch(j -> Objects.equals(f.getSubSelect().getAlias(), j.getAlias())))
                        .collect(Collectors.toList());

        List<RdbmsQueryJoin> subSelectFilterFeaturesQueryJoins = subSelectFilterFeaturesNotProcessed.stream()
                .map(f -> RdbmsQueryJoin.<ID>builder()
                        .resultSet(
                                RdbmsResultSet.<ID>builder()
                                        .level(builderContext.getLevel() + 1)
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
                .collect(Collectors.toList());

        joins.addAll(subSelectFilterFeaturesQueryJoins);
        conditions.addAll(rdbmsBuilder.mapFeatureToRdbms(filter.getFeature(), builderContext).collect(Collectors.toList()));
        rdbmsBuilder.addAncestorJoins(joins, filter, ancestors);
    }

}
