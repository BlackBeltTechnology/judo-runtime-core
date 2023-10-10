package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

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
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.meta.query.TypeAttribute;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsColumn;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.UniqueEList;
import org.eclipse.emf.ecore.EClass;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class TypeAttributeMapper extends RdbmsMapper<TypeAttribute> {

    @Override
    public Stream<RdbmsColumn> map(final TypeAttribute typeAttribute, RdbmsBuilder.RdbmsBuilderContext context) {
        final EClass sourceType = typeAttribute.getNode().getType();
        sourceType.getEAllSuperTypes().forEach(superType -> {
            log.trace("   - found super type: {}", AsmUtils.getClassifierFQName(superType));
            if (!context.ancestors.containsKey(typeAttribute.getNode())) {
                context.ancestors.put(typeAttribute.getNode(), new UniqueEList<>());
            }
            // add ancestor for a given attribute
            context.ancestors.get(typeAttribute.getNode()).add(superType);
        });

        return getTargets(typeAttribute)
                .flatMap(t -> Arrays.asList(
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_TYPE_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_TYPE_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_VERSION_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_VERSION_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_CREATE_USERNAME_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_CREATE_USERNAME_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_CREATE_USER_ID_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_CREATE_USER_ID_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_CREATE_TIMESTAMP_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_CREATE_TIMESTAMP_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_UPDATE_USERNAME_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_UPDATE_USERNAME_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_UPDATE_USER_ID_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_UPDATE_USER_ID_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_UPDATE_TIMESTAMP_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_UPDATE_TIMESTAMP_COLUMN_NAME)
                                .build()
                ).stream());
    }
}
