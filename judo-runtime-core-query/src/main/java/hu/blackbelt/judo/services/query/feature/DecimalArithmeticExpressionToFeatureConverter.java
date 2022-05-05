package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.DecimalArithmeticExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;

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
