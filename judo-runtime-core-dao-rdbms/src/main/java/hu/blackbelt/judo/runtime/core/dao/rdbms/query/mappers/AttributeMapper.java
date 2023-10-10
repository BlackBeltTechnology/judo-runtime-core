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
import hu.blackbelt.judo.meta.query.Attribute;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsColumn;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.UniqueEList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;

import java.util.Map;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class AttributeMapper<ID> extends RdbmsMapper<Attribute> {

    @Override
    public Stream<RdbmsColumn> map(final Attribute attribute, RdbmsBuilder.RdbmsBuilderContext context) {
        final EClass sourceType = attribute.getNode().getType();
        final EAttribute sourceAttribute = attribute.getSourceAttribute();
        final EClass attributeContainer = sourceAttribute.getEContainingClass();

        log.trace("Checking attributes");
        log.trace(" - source type: {}", sourceType.getName());
        log.trace(" - source attribute: {}", sourceAttribute.getName());
        log.trace(" - source attribute container: {}", attributeContainer.getName());

        // add column to list
        final String postfix;
        if (!AsmUtils.equals(sourceType, attributeContainer)) {  // inherited attribute
            log.trace("   - found inherited attribute: {}", sourceAttribute.getName());

            if (!context.ancestors.containsKey(attribute.getNode())) {
                context.ancestors.put(attribute.getNode(), new UniqueEList<>());
            }
            // add ancestor for a given attribute
            context.ancestors.get(attribute.getNode()).add(attributeContainer);
            postfix = context.rdbmsBuilder.getAncestorPostfix(attributeContainer);
        } else {
            postfix = "";
        }

        return getTargets(attribute).map(t -> RdbmsColumn.builder()
                .partnerTable(attribute.getNode())
                .partnerTablePostfix(postfix)
                .columnName(context.rdbmsBuilder.getColumnName(attribute.getSourceAttribute()))
                .target(t.getTarget())
                .targetAttribute(t.getTargetAttribute())
                .alias(t.getAlias())
                .sourceDomainConstraints(RdbmsField.getDomainConstraints(attribute.getSourceAttribute()))
                .build());
    }
}
