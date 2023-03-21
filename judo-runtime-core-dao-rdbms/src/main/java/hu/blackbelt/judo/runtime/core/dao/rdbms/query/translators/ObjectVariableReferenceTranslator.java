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

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.object.ObjectVariableReference;
import hu.blackbelt.judo.meta.expression.variable.ObjectVariable;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.expression.object.util.builder.ObjectBuilders.newObjectVariableReferenceBuilder;

@Builder
public class ObjectVariableReferenceTranslator implements Function<ObjectVariableReference, ObjectVariableReference> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public ObjectVariableReference apply(final ObjectVariableReference objectVariableReference) {
        checkArgument(objectVariableReference.getVariable() instanceof Expression, "Expression variables are supported only");
        return newObjectVariableReferenceBuilder()
                .withVariable((ObjectVariable) translator.apply((Expression) objectVariableReference.getVariable()))
                .build();
    }
}
