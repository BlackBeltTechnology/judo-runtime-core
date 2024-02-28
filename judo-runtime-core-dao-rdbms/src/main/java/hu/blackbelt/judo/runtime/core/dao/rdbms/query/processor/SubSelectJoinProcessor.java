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

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.RdbmsMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsResultSet;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsQueryJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsTableJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

@Builder
@Slf4j
public class SubSelectJoinProcessor<ID> {
    @NonNull
    private final RdbmsResolver rdbmsResolver;

    public List<RdbmsJoin> process(SubSelectJoin join, boolean withoutFeatures, Map<String, Object> mask, RdbmsBuilderContext builderContext) {
        final RdbmsBuilder rdbmsBuilder = builderContext.getRdbmsBuilder();
        final SubSelect subSelect = join.getSubSelect();

        if (log.isTraceEnabled()) {
            log.trace("Join:            " + join.toString());
            log.trace("WithoutFeatures: " + withoutFeatures);
            log.trace("Mask:            " + (mask == null ? "" : mask));
            log.trace(builderContext.toString());
        }

        subSelect.getFilters().addAll(join.getFilters());

        final List<RdbmsJoin> joins = new ArrayList<>();
        final Map<String, Object> _mask = mask != null &&
                subSelect.getTransferRelation() != null
                ? (Map<String, Object>) mask.get(subSelect.getTransferRelation().getName())
                : null;

        final RdbmsResultSet<ID> resultSetHandler =
                RdbmsResultSet.<ID>builder()
                        .query(subSelect)
                        .builderContext(builderContext)
                        .withoutFeatures(withoutFeatures)
                        .mask(_mask)
                        .build();

        if (!Objects.equals(((SubSelectJoin) join).getPartner(), subSelect.getBase())) {
            joins.add(RdbmsTableJoin.builder()
                    .tableName(rdbmsBuilder.getTableName(subSelect.getBase().getType()))
                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                    .partnerTable(((SubSelectJoin) join).getPartner())
                    .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                    .alias(subSelect.getBase().getAlias())
                    .build());
        }

        joins.add(RdbmsQueryJoin.<ID>builder()
                .resultSet(resultSetHandler)
                .outer(true)
                .columnName(RdbmsAliasUtil.getOptionalParentIdColumnAlias(subSelect.getContainer()))
                .partnerTable(subSelect.getBase())
                .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                .alias(subSelect.getAlias())
                .build());

        final Optional<Feature> selectorFeature = subSelect.getSelect().getFeatures().stream()
                .filter(f -> (f instanceof Function) && (FunctionSignature.MIN_INTEGER.equals(((Function) f).getSignature()) || FunctionSignature.MAX_INTEGER.equals(((Function) f).getSignature())))
                .findAny();
        checkArgument(selectorFeature.isPresent(), "SubSelectFeature of head/tail/any must exists");

        final Optional<RdbmsMapper.RdbmsTarget> selectorTarget = RdbmsMapper.getTargets(selectorFeature.get()).findAny();
        checkArgument(selectorTarget.isPresent(), "SubSelectFeature target must exists");

        joins.add(RdbmsTableJoin.builder()
                .tableName(rdbmsResolver.rdbmsTable(join.getType()).getSqlName())
                .alias(join.getAlias())
                .columnName(StatementExecutor.ID_COLUMN_NAME)
                .partnerTable(subSelect)
                .partnerColumnName(selectorTarget.get().getAlias() + "_" + selectorTarget.get().getTarget().getIndex())
                .outer(true)
                .build());

        if (builderContext.getAncestors().containsKey(join)) {
            builderContext.getRdbmsBuilder().addAncestorJoins(joins, join, builderContext);
        }
        return joins;
    }
}
