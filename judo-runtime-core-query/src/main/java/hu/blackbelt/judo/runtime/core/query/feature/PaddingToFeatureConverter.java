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
import hu.blackbelt.judo.meta.expression.string.PaddignType;
import hu.blackbelt.judo.meta.expression.string.PaddingExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.util.builder.QueryBuilders;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class PaddingToFeatureConverter extends ExpressionToFeatureConverter<PaddingExpression> {

    public PaddingToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final PaddingExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        FunctionSignature functionSignature;
        if (PaddignType.LEFT.equals(expression.getPaddingType())) {
            functionSignature = FunctionSignature.LEFT_PAD;
        } else if (PaddignType.RIGHT.equals(expression.getPaddingType())) {
            functionSignature = FunctionSignature.RIGHT_PAD;
        } else {
            throw new IllegalArgumentException("Unknown padding type: " + expression.getPaddingType());
        }
        final Feature feature = QueryBuilders.newFunctionBuilder()
                                             .withSignature(functionSignature)
                                             .withParameters(newFunctionParameterBuilder()
                                                                     .withParameterName(ParameterName.STRING)
                                                                     .withParameterValue(factory.convert(expression.getExpression(), context, null))
                                                                     .build(),
                                                             newFunctionParameterBuilder()
                                                                     .withParameterName(ParameterName.LENGTH)
                                                                     .withParameterValue(factory.convert(expression.getLength(), context, null))
                                                                     .build(),
                                                             newFunctionParameterBuilder()
                                                                     .withParameterName(ParameterName.REPLACEMENT)
                                                                     .withParameterValue(factory.convert(expression.getPadding(), context, null))
                                                                     .build()
                                             )
                                             .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                                             .build();

        context.addFeature(feature);
        return feature;
    }

}
