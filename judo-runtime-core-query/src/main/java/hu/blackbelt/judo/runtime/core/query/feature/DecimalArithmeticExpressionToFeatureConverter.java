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
import hu.blackbelt.judo.meta.expression.numeric.DecimalArithmeticExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class DecimalArithmeticExpressionToFeatureConverter extends ExpressionToFeatureConverter<DecimalArithmeticExpression> {

    public DecimalArithmeticExpressionToFeatureConverter(final FeatureFactory factory, final AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final DecimalArithmeticExpression decimalArithmeticExpression, final Context context, final FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        switch (decimalArithmeticExpression.getOperator()) {
            case ADD:
                signature = FunctionSignature.ADD_DECIMAL;
                break;
            case SUBSTRACT:
                signature = FunctionSignature.SUBTRACT_DECIMAL;
                break;
            case MULTIPLY:
                signature = FunctionSignature.MULTIPLE_DECIMAL;
                break;
            case DIVIDE:
                signature = FunctionSignature.DIVIDE_DECIMAL;
                break;
            case MODULO:
                signature = FunctionSignature.MODULO_DECIMAL;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported decimal operation");
        }

        final Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.LEFT)
                        .withParameterValue(factory.convert(decimalArithmeticExpression.getLeft(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.RIGHT)
                        .withParameterValue(factory.convert(decimalArithmeticExpression.getRight(), context, null))
                        .build())
                .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                .build();

        context.addFeature(feature);
        return feature;
    }
}
