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
import hu.blackbelt.judo.meta.expression.numeric.ExtractTimeExpression;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.meta.query.FunctionSignature;
import hu.blackbelt.judo.meta.query.ParameterName;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class ExtractTimeExpressionToFeatureConverter extends ExpressionToFeatureConverter<ExtractTimeExpression> {

    public ExtractTimeExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final ExtractTimeExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        switch (expression.getPart()) {
            case HOUR: {
                signature = FunctionSignature.HOURS_OF_TIME;
                break;
            }
            case MINUTE: {
                signature = FunctionSignature.MINUTES_OF_TIME;
                break;
            }
            case SECOND: {
                signature = FunctionSignature.SECONDS_OF_TIME;
                break;
            }
            case MILLISECOND: {
                signature = FunctionSignature.MILLISECONDS_OF_TIME;
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported time part");
        }

        final Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.TIME)
                        .withParameterValue(factory.convert(expression.getTime(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
