package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.IntegerArithmeticExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class IntegerArithmeticExpressionToFeatureConverter extends ExpressionToFeatureConverter<IntegerArithmeticExpression> {

    public IntegerArithmeticExpressionToFeatureConverter(final FeatureFactory factory, final AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final IntegerArithmeticExpression integerArithmeticExpression, final Context context, final FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        switch (integerArithmeticExpression.getOperator()) {
            case ADD:
                signature = FunctionSignature.ADD_INTEGER;
                break;
            case SUBSTRACT:
                signature = FunctionSignature.SUBTRACT_INTEGER;
                break;
            case MULTIPLY:
                signature = FunctionSignature.MULTIPLE_INTEGER;
                break;
            case DIVIDE:
                signature = FunctionSignature.DIVIDE_INTEGER;
                break;
            case MODULO:
                signature = FunctionSignature.MODULO_INTEGER;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported integer operation");
        }

        final Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.LEFT)
                        .withParameterValue(factory.convert(integerArithmeticExpression.getLeft(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.RIGHT)
                        .withParameterValue(factory.convert(integerArithmeticExpression.getRight(), context, null))
                        .build())
                .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                .build();

        context.addFeature(feature);
        return feature;
    }
}
