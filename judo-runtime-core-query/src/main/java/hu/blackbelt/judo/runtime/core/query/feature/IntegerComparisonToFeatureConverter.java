package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.logical.IntegerComparison;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class IntegerComparisonToFeatureConverter extends ExpressionToFeatureConverter<IntegerComparison> {

    public IntegerComparisonToFeatureConverter(final FeatureFactory factory, final AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final IntegerComparison integerComparison, final Context context, final FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        switch (integerComparison.getOperator()) {
            case EQUAL:
                signature = FunctionSignature.EQUALS;
                break;
            case NOT_EQUAL:
                signature = FunctionSignature.NOT_EQUALS;
                break;
            case LESS_THAN:
                signature = FunctionSignature.LESS_THAN;
                break;
            case LESS_OR_EQUAL:
                signature = FunctionSignature.LESS_OR_EQUAL;
                break;
            case GREATER_OR_EQUAL:
                signature = FunctionSignature.GREATER_OR_EQUAL;
                break;
            case GREATER_THAN:
                signature = FunctionSignature.GREATER_THAN;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported integer comparison");
        }

        final Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.LEFT)
                        .withParameterValue(factory.convert(integerComparison.getLeft(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.RIGHT)
                        .withParameterValue(factory.convert(integerComparison.getRight(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
