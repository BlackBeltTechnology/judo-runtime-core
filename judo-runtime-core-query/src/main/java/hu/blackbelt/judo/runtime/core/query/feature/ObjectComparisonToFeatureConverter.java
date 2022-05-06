package hu.blackbelt.judo.runtime.core.query.feature;

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
