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
import hu.blackbelt.judo.meta.expression.logical.ObjectComparison;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.util.builder.QueryBuilders;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class ObjectComparisonToFeatureConverter extends ExpressionToFeatureConverter<ObjectComparison> {

    public ObjectComparisonToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final ObjectComparison objectComparison, final Context context, final FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        switch (objectComparison.getOperator()) {
            case EQUAL:
                signature = FunctionSignature.EQUALS;
                break;
            case NOT_EQUAL:
                signature = FunctionSignature.NOT_EQUALS;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported object comparison");
        }

        final Feature feature = QueryBuilders.newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.LEFT)
                        .withParameterValue(factory.convert(objectComparison.getLeft(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.RIGHT)
                        .withParameterValue(factory.convert(objectComparison.getRight(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
