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

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.DecimalRoundExpression;
import hu.blackbelt.judo.meta.expression.numeric.IntegerRoundExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.util.builder.QueryBuilders;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

public class RoundToFeatureConverter extends ExpressionToFeatureConverter<Expression> {

    public RoundToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final Expression expression, final Context context, final FeatureTargetMapping targetMapping) {

        final Feature feature;

        if (expression instanceof IntegerRoundExpression) {
            feature = QueryBuilders.newFunctionBuilder()
                                   .withSignature(FunctionSignature.INTEGER_ROUND)
                                   .withParameters(QueryBuilders.newFunctionParameterBuilder()
                                                                .withParameterName(ParameterName.NUMBER)
                                                                .withParameterValue(factory.convert(((IntegerRoundExpression) expression).getExpression(), context, null))
                                                                .build())
                                   .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                                   .build();
        } else if (expression instanceof DecimalRoundExpression) {
            feature = QueryBuilders.newFunctionBuilder()
                                   .withSignature(FunctionSignature.DECIMAL_ROUND)
                                   .withParameters(QueryBuilders.newFunctionParameterBuilder()
                                                                .withParameterName(ParameterName.NUMBER)
                                                                .withParameterValue(factory.convert(((DecimalRoundExpression) expression).getExpression(), context, null))
                                                                .build(),
                                                   QueryBuilders.newFunctionParameterBuilder()
                                                                .withParameterName(ParameterName.POSITION)
                                                                .withParameterValue(factory.convert(((DecimalRoundExpression) expression).getScale(), context, null))
                                                                .build())
                                   .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                                   .build();
        } else {
            throw new IllegalArgumentException("Unsupported expression: " + expression.getClass().getSimpleName());
        }

        context.addFeature(feature);
        return feature;
    }

}
