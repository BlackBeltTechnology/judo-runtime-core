package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import hu.blackbelt.judo.meta.query.Filter;
import hu.blackbelt.judo.meta.query.Join;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.Select;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.meta.query.SubSelectFeature;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsResultSet;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsQueryJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsTableJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.util.EcoreUtil;

import static hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil.getParentIdColumnAlias;

@Builder
@Slf4j
public class FilterJoinProcessor {

    public void process(final FilterJoinProcessorParameters params, final RdbmsBuilderContext builderContext) {
        final RdbmsBuilder rdbmsBuilder = (RdbmsBuilder) builderContext.getRdbmsBuilder();
        final List<RdbmsJoin> joins = params.getJoins();
        final Filter filter = params.getFilter();
        final SubSelect query = params.getQuery();
        final List<Join> processedNodesForJoins = params.getProcessedNodesForJoins();
        final List<RdbmsField> conditions = params.getConditions();
        final String partnerTablePrefix = params.getPartnerTablePrefix();
        final Node partnerTable = params.getPartnerTable();
        final Boolean addJoinsOfFilterFeature = params.isAddJoinsOfFilterFeature();

        if (log.isTraceEnabled()) {
            log.trace(params.toString());
            log.trace(builderContext.toString());
        }

        int sizeOfJoinsBeforeFilterJoins = joins.size();

        if (joins.stream().noneMatch(j -> Objects.equals(filter.getAlias(), j.getAlias()))) {
            joins.add(RdbmsTableJoin.builder()
                                    .tableName(rdbmsBuilder.getTableName(filter.getType()))
                                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                                    .partnerTablePrefix(partnerTablePrefix)
                                    .partnerTable(partnerTable)
                                    .partnerColumnName(partnerTable instanceof SubSelect ? getParentIdColumnAlias(query.getContainer()) : StatementExecutor.ID_COLUMN_NAME)
                                    .alias(filter.getAlias())
                                    .build());
            if (addJoinsOfFilterFeature) {
                List<Join> filterFeaturesNotProcessed =
                        filter.getFeature().getNodes().stream()
                              .filter(n -> !Objects.equals(n, filter) && n instanceof Join)
                              .flatMap(n -> ((Join) n).getAllJoins().stream())
                              .toList();

                for (Join join : filterFeaturesNotProcessed) {
                    Set<Join> allPartnerJoins =
                            filterFeaturesNotProcessed.stream()
                                                      .filter(j -> !processedNodesForJoins.contains(j))
                                                      .flatMap(j -> getPartnerJoins(j).stream())
                                                      .collect(Collectors.toSet());
                    if (!processedNodesForJoins.contains(join) && !allPartnerJoins.contains(join)) { // process join tree from the bottom
                        if (filter.eContainer() instanceof Select selectOfFilter && selectOfFilter.eContainer() == null) {
                            // In the case of filters added by the query customizer (not JQL filter expressions), the filter's container is a Select,
                            // and that Select does not have its own container
                            processJoinTreeForFilter(builderContext, join, processedNodesForJoins, filter, joins, rdbmsBuilder);
                        } else {
                            processExistingJoinTree(builderContext, join, processedNodesForJoins, joins, rdbmsBuilder);
                        }
                    }
                }
            }
        }

        List<SubSelectFeature> subSelectFilterFeaturesNotProcessed =
                filter.getFeatures().stream()
                      .filter(f -> f instanceof SubSelectFeature).map(f -> (SubSelectFeature) f)
                      .filter(f -> joins.stream().noneMatch(j -> Objects.equals(f.getSubSelect().getAlias(), j.getAlias())))
                      .toList();

        List<RdbmsQueryJoin> subSelectFilterFeaturesQueryJoins =
                subSelectFilterFeaturesNotProcessed.stream()
                                                   .map(f -> RdbmsQueryJoin.builder()
                                                                           .resultSet(RdbmsResultSet.builder()
                                                                                                    .query(f.getSubSelect())
                                                                                                    .builderContext(builderContext)
                                                                                                    .withoutFeatures(true)
                                                                                                    .build())
                                                                           .outer(true)
                                                                           .columnName(RdbmsAliasUtil.getOptionalParentIdColumnAlias(f.getSubSelect().getContainer()))
                                                                           .partnerTable(f.getSubSelect().getNavigationJoins().isEmpty() ? null : f.getSubSelect().getContainer())
                                                                           .partnerColumnName(f.getSubSelect().getNavigationJoins().isEmpty() ? null : StatementExecutor.ID_COLUMN_NAME)
                                                                           .alias(f.getSubSelect().getAlias())
                                                                           .build())
                                                   .collect(Collectors.toList());

        joins.addAll(subSelectFilterFeaturesQueryJoins);

        int sizeOfJoinsAfterFilterJoins = joins.size();

        conditions.addAll(rdbmsBuilder.mapFeatureToRdbms(filter.getFeature(), builderContext).toList());
        rdbmsBuilder.addAncestorJoins(joins, filter, builderContext);

        int sizeOfJoinsAfterAncestors = joins.size();

        List<RdbmsJoin> newJoinsList = new ArrayList<>(joins);
        joins.clear();

        joins.addAll(newJoinsList.subList(0, sizeOfJoinsBeforeFilterJoins));
        joins.addAll(newJoinsList.subList(sizeOfJoinsAfterFilterJoins, sizeOfJoinsAfterAncestors));
        joins.addAll(newJoinsList.subList(sizeOfJoinsBeforeFilterJoins, sizeOfJoinsAfterFilterJoins));
    }

    private static Collection<Join> getPartnerJoins(Join join) {
        List<Join> joins = new ArrayList<>();
        Node currentNode = join.getPartner();
        while (currentNode instanceof Join currentJoin) {
            joins.add(currentJoin);
            currentNode = currentJoin.getPartner();
        }
        return joins;
    }

    private static void processJoinTreeForFilter(RdbmsBuilderContext builderContext, Join join, List<Join> processedNodesForJoins, Filter filter, List<RdbmsJoin> joins, RdbmsBuilder rdbmsBuilder) {
        Stack<Join> joinStack = new Stack<>();
        Node currentNode = join;
        while (currentNode instanceof Join currentJoin) {
            Join newJoin = EcoreUtil.copy(currentJoin);
            processedNodesForJoins.add(currentJoin);
            processedNodesForJoins.add(newJoin);

            joinStack.push(newJoin);
            currentNode = currentJoin.getPartner();
        }

        Join previousJoin;
        Join stackElement = null;
        while (!joinStack.isEmpty()) {
            previousJoin = stackElement;
            stackElement = joinStack.pop();

            stackElement.setPartner(previousJoin == null ? filter : previousJoin);
            addJoin(builderContext, joins, rdbmsBuilder, stackElement);
        }
    }

    private static void processExistingJoinTree(RdbmsBuilderContext builderContext, Join join, List<Join> processedNodesForJoins, List<RdbmsJoin> joins, RdbmsBuilder rdbmsBuilder) {
        Stack<Join> joinStack = new Stack<>();
        Node currentNode = join;
        while (currentNode instanceof Join currentJoin) {
            processedNodesForJoins.add(currentJoin);

            joinStack.push(currentJoin);
            currentNode = currentJoin.getPartner();
        }

        while (!joinStack.isEmpty()) {
            addJoin(builderContext, joins, rdbmsBuilder, joinStack.pop());
        }
    }

    private static void addJoin(RdbmsBuilderContext builderContext, List<RdbmsJoin> joins, RdbmsBuilder rdbmsBuilder, Join join) {
        joins.addAll(rdbmsBuilder.processJoin(
                JoinProcessParameters.builder()
                                     .builderContext(builderContext)
                                     .join(join)
                                     .build()
        ));
    }

}
