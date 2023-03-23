package hu.blackbelt.judo.runtime.core.dao.core.statements;

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
import hu.blackbelt.judo.runtime.core.dao.core.values.AttributeValue;
import hu.blackbelt.judo.runtime.core.dao.core.values.InstanceValue;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EClass;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class CheckUniqueAttributeStatement<ID> extends Statement<ID> {

    @Builder(builderMethodName = "buildCheckIdentifierStatement")
    public CheckUniqueAttributeStatement(
            EClass type,
            ID identifier
    ) {

        super(InstanceValue
                        .<ID>buildInstanceValue()
                            .type(type)
                            .identifier(identifier)
                            .build());
    }

    public static <ID> CheckUniqueAttributeStatement<ID> fromStatement(Statement<ID> statement) {
        CheckUniqueAttributeStatement uniqueStatement = CheckUniqueAttributeStatement.<ID>buildCheckIdentifierStatement()
                .identifier(statement.getInstance().getIdentifier())
                .type(statement.getInstance().getType())
                .build();
        statement.getInstance().getAttributes()
                .forEach(a -> uniqueStatement.getInstance().addAttributeValue(a.getAttribute(), a.getValue()));
        return uniqueStatement;
    }

    public CheckUniqueAttributeStatement mergeAttributes(Statement<ID> statement) {
        List<AttributeValue<Object>> updatedAttributes = new ArrayList<>(this.getInstance().getAttributes());
        List<AttributeValue> attributeToRemove = updatedAttributes.stream()
                .filter(e -> AsmUtils.isIdentifier(e.getAttribute()))
                .filter(a -> statement.getInstance().getAttributes().stream().map(sa -> sa.getAttribute()).collect(Collectors.toSet())
                                .contains(a.getAttribute())).collect(Collectors.toList());
        updatedAttributes.removeAll(attributeToRemove);
        updatedAttributes.addAll(statement.getInstance().getAttributes());

        CheckUniqueAttributeStatement uniqueStatement = CheckUniqueAttributeStatement.<ID>buildCheckIdentifierStatement()
                .identifier(statement.getInstance().getIdentifier())
                .type(statement.getInstance().getType())
                .build();

        updatedAttributes.forEach(a -> this.getInstance().addAttributeValue(a.getAttribute(), a.getValue()));
        return uniqueStatement;
    }


    public String toString() {
        return "CheckUniqueAttributeStatement(" + this.instance.toString() +  ")";
    }
}
