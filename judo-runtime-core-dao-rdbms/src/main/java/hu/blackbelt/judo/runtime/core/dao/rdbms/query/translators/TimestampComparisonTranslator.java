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
import hu.blackbelt.judo.meta.expression.TimestampExpression;
import hu.blackbelt.judo.meta.expression.logical.TimestampComparison;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newTimestampComparisonBuilder;

@Builder
public class TimestampComparisonTranslator implements Function<TimestampComparison, TimestampComparison> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public TimestampComparison apply(final TimestampComparison timestampComparator) {
        return newTimestampComparisonBuilder()
                .withLeft((TimestampExpression) translator.apply(timestampComparator.getLeft()))
                .withOperator(timestampComparator.getOperator())
                .withRight((TimestampExpression) translator.apply(timestampComparator.getRight()))
                .build();
    }
}
