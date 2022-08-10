package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

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
import hu.blackbelt.judo.meta.expression.constant.Instance;
import lombok.Builder;
import lombok.NonNull;
import org.eclipse.emf.ecore.EClass;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.constant.util.builder.ConstantBuilders.newInstanceBuilder;
import static hu.blackbelt.judo.meta.expression.util.builder.ExpressionBuilders.newTypeNameBuilder;

@Builder
public class InstanceTranslator implements Function<Instance, Instance> {

    @NonNull
    private final AsmUtils asmUtils;

    @Override
    public Instance apply(final Instance instance) {
        final String name = instance.getElementName().getName();
        final String namespace = instance.getElementName().getNamespace();

        final EClass transferObjectType = asmUtils.getClassByFQName(namespace.replace("::", ".") + "." + name)
                .orElseThrow(() -> new IllegalStateException("Transfer object type not found: " + instance));
        final EClass entityType = asmUtils.getMappedEntityType(transferObjectType)
                .orElseThrow(() -> new IllegalStateException("Entity type not found: " + instance));

        final String entityName = entityType.getName();
        final String entityNamespace = AsmUtils.getPackageFQName(entityType.getEPackage());

        return newInstanceBuilder()
                .withName("self")
                .withElementName(newTypeNameBuilder()
                        .withName(entityName)
                        .withNamespace(entityNamespace)
                        .build())
                .build();
    }
}
