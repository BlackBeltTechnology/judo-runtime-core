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
import hu.blackbelt.judo.meta.query.IdAttribute;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsColumn;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.UniqueEList;
import org.eclipse.emf.ecore.EClass;

import java.util.stream.Stream;

@Slf4j
public class IdAttributeMapper extends RdbmsMapper<IdAttribute> {

    @Override
    public Stream<RdbmsColumn> map(final IdAttribute idAttribute, RdbmsBuilderContext context) {
        final EMap<Node, EList<EClass>> ancestors = context.ancestors;

        final EClass sourceType = idAttribute.getNode().getType();
        for (EClass superType : sourceType.getEAllSuperTypes()) {
            log.trace("   - found super type: {}", AsmUtils.getClassifierFQName(superType));
            if (!ancestors.containsKey(idAttribute.getNode())) {
                ancestors.put(idAttribute.getNode(), new UniqueEList<>());
            }
            // add ancestor for a given attribute
            ancestors.get(idAttribute.getNode()).add(superType);
        }
        return getTargets(idAttribute)
                .map(t -> RdbmsColumn.builder()
                        .partnerTable(idAttribute.getNode())
                        .columnName(StatementExecutor.ID_COLUMN_NAME)
                        .alias(idAttribute.getNode().getAlias() + "_" + StatementExecutor.ID_COLUMN_NAME)
                        .build());
    }
}
