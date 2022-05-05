package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.logical.TimestampComparison;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class TimestampComparisonToFeatureConverter extends ExpressionToFeatureConverter<TimestampComparison> {

    public TimestampComparisonToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final TimestampComparison timestampComparison, final Context context, final FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        switch (timestampComparison.getOperator()) {
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
                throw new UnsupportedOperationException("Unsupported timestamp comparison");
        }

        final Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.LEFT)
                        .withParameterValue(factory.convert(timestampComparison.getLeft(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.RIGHT)
                        .withParameterValue(factory.convert(timestampComparison.getRight(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
