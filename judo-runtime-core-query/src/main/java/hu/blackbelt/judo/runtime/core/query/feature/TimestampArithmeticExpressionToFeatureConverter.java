package hu.blackbelt.judo.runtime.core.query.feature;

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

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.temporal.TimestampArithmeticExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.util.builder.QueryBuilders;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import java.util.List;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;

public class TimestampArithmeticExpressionToFeatureConverter extends ExpressionToFeatureConverter<TimestampArithmeticExpression> {

    public TimestampArithmeticExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final TimestampArithmeticExpression expression, final Context context, final FeatureTargetMapping targetMapping) {

        final FunctionSignature signature;
        switch (expression.getTimestampPart()) {
            case YEAR:
                signature = FunctionSignature.TIMESTAMP_PLUS_YEARS;
                break;
            case MONTH:
                signature = FunctionSignature.TIMESTAMP_PLUS_MONTHS;
                break;
            case DAY:
                signature = FunctionSignature.TIMESTAMP_PLUS_DAYS;
                break;
            case HOUR:
                signature = FunctionSignature.TIMESTAMP_PLUS_HOURS;
                break;
            case MINUTE:
                signature = FunctionSignature.TIMESTAMP_PLUS_MINUTES;
                break;
            case SECOND:
                signature = FunctionSignature.TIMESTAMP_PLUS_SECONDS;
                break;
            case MILLISECOND:
                signature = FunctionSignature.TIMESTAMP_PLUS_MILLISECONDS;
                break;
            default:
                throw new IllegalArgumentException("Unsupported timestamp part: " + expression.getTimestampPart());
        }

        Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(List.of(
                        QueryBuilders.newFunctionParameterBuilder()
                                     .withParameterName(ParameterName.NUMBER)
                                     .withParameterValue(factory.convert(expression.getAmount(), context, null))
                                     .build(),
                        QueryBuilders.newFunctionParameterBuilder()
                                     .withParameterName(ParameterName.TIMESTAMP)
                                     .withParameterValue(factory.convert(expression.getTimestamp(), context, null))
                                     .build()
                ))
                .build();
        context.addFeature(feature);
        return feature;
    }

}
