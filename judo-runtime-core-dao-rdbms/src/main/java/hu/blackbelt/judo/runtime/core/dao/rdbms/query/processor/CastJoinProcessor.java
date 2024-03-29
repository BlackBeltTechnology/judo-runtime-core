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

import hu.blackbelt.judo.meta.query.CastJoin;
import hu.blackbelt.judo.meta.query.Filter;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelectFeature;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsFunction;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsNavigationFilter;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsTableJoin;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Builder
@Slf4j
public class CastJoinProcessor<ID> {

    @NonNull
    private final RdbmsResolver rdbmsResolver;

    public List<RdbmsJoin> process(Node join, RdbmsBuilderContext builderContext) {
        if (log.isTraceEnabled()) {
            log.trace("Node:       " + join);
            log.trace(builderContext.toString());
        }

        final RdbmsBuilder<?> rdbmsBuilder = builderContext.getRdbmsBuilder();

        EClass castTargetType = join.getType();
        Set<EClass> typeSet = new HashSet<>(castTargetType.getEAllSuperTypes());
        typeSet.add(castTargetType);

        List<EClass> types = typeSet.stream()
                .sorted((l, r) -> {
                    // ascending order
                    // type < supertype
                    if (l.getEAllSuperTypes().contains(r)) {
                        return -1;
                    }
                    if (r.getEAllSuperTypes().contains(l)) {
                        return 1;
                    }
                    return 0;
                })
                .collect(Collectors.toUnmodifiableList());

        List<RdbmsJoin> rdbmsTableJoins = new ArrayList<>();
        for (EClass type : types) {
            String alias = join.getAlias();
            Node partnerTable = ((CastJoin) join).getPartner();
            List<RdbmsFunction> onConditions = new ArrayList<>();
            RdbmsTableJoin rdbmsPartnerTable = null;

            if (rdbmsTableJoins.isEmpty()) {
                // original, first join
                List<Filter> joinFilters = join.getFilters();
                boolean joinFiltersWithoutSubSelectFeatures =
                        !joinFilters.isEmpty()
                                && joinFilters.stream()
                                .noneMatch(filter -> filter.getFeatures().stream()
                                        .anyMatch(feature -> feature instanceof SubSelectFeature));

                if (joinFiltersWithoutSubSelectFeatures) {
                    onConditions = joinFilters.stream()
                            .map(f -> RdbmsFunction.builder()
                                    .pattern("EXISTS ({0})")
                                    .parameter(RdbmsNavigationFilter.<ID>builder()
                                            .builderContext(builderContext)
                                            .filter(f)
                                            .build())
                                    .build())
                            .collect(Collectors.toList());
                }
            } else {
                // additional join for supertypes
                alias += rdbmsBuilder.getAncestorPostfix(type);
                partnerTable = null;
                rdbmsPartnerTable = (RdbmsTableJoin) rdbmsTableJoins.get(rdbmsTableJoins.size() - 1);
            }

            rdbmsTableJoins.add(RdbmsTableJoin.builder()
                    .outer(true)
                    .alias(alias)
                    .partnerTable(partnerTable)
                    .onConditions(onConditions)
                    .rdbmsPartnerTable(rdbmsPartnerTable)
                    .columnName(SelectStatementExecutor.ID_COLUMN_NAME)
                    .tableName(rdbmsResolver.rdbmsTable(type).getSqlName())
                    .partnerColumnName(SelectStatementExecutor.ID_COLUMN_NAME)
                    .build());
        }

        return rdbmsTableJoins;
    }
}
