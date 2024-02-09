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
import hu.blackbelt.judo.meta.query.ContainerJoin;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsContainerJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Builder
public class ContainerJoinProcessor {
    @NonNull
    private final RdbmsResolver rdbmsResolver;

    public List<RdbmsJoin> process(ContainerJoin join, RdbmsBuilderContext builderContext) {
        final RdbmsBuilder<?> rdbmsBuilder = builderContext.getRdbmsBuilder();

        final EClass targetType = join.getType();
        final Node node = join.getPartner();
        final List<EReference> references = join.getReferences();
        final EClass sourceType = node != null ? node.getType() : references.get(0).getEReferenceType();

        if (log.isTraceEnabled()) {
            log.trace(" => processing JOIN: {}", join);
            log.trace("    target type: {}", targetType.getName());
            log.trace("    source type: {}", sourceType.getName());
            log.trace("    references: {}", references.stream().map(r -> AsmUtils.getReferenceFQName(r)).collect(Collectors.joining(", ")));
            log.trace(builderContext.toString());
        }

        final List<RdbmsJoin> result = new ArrayList<>();
        int index = 0;
        for (final EReference r : join.getReferences()) {
            result.addAll(rdbmsBuilder.processSimpleJoin(SimpleJoinProcessorParameters.builder()
                    .postfix(RdbmsContainerJoin.POSTFIX + index++)
                    .join(join)
                    .opposite(r)
                    .builderContext(builderContext)
                    .build()));
        }

        result.add(RdbmsContainerJoin.builder()
                .outer(true)
                .tableName(rdbmsResolver.rdbmsTable(targetType).getSqlName())
                .alias(join.getAlias())
                .partnerTable(node)
                .columnName(StatementExecutor.ID_COLUMN_NAME)
                .references(references)
                .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                .build());

        return result;
    }
}
