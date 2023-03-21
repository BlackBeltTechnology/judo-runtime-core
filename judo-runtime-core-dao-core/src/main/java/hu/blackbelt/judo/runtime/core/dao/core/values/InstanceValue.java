package hu.blackbelt.judo.runtime.core.dao.core.values;

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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Represents a statement instance. It can contain attributes and references. All statement have minimum one instance
 * to work with.
 * @param <ID>
 */
@Builder(builderMethodName = "buildInstanceValue")
@Getter
@EqualsAndHashCode
public class InstanceValue<ID> {
    @NonNull
    EClass type;

    @NonNull
    ID identifier;

    @Builder.Default
    List<AttributeValue<Object>> attributes = newArrayList();

    public void addAttributeValue(EAttribute attribute, Object value) {
        attributes.add(AttributeValue
                .<Object>attributeValueBuilder()
                    .attribute(attribute)
                    .value(value)
                    .build());
    }

    public String toString() {
        return "InstanceValue(type=" + AsmUtils.getClassifierFQName(this.getType()) +
                ", identifier=" + this.getIdentifier() +
                ", attributes=" + this.getAttributes() +
                ")";
    }
}
