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
import hu.blackbelt.judo.meta.expression.IntegerExpression;
import hu.blackbelt.judo.meta.expression.numeric.IntegerOppositeExpression;
import hu.blackbelt.judo.meta.expression.numeric.util.builder.IntegerOppositeExpressionBuilder;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

@Builder
public class IntegerOppositeTranslator implements Function<IntegerOppositeExpression, IntegerOppositeExpression> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public IntegerOppositeExpression apply(final IntegerOppositeExpression expression) {
        return IntegerOppositeExpressionBuilder.create()
                .withExpression((IntegerExpression) translator.apply(expression.getExpression()))
                .build();
    }
}
