package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.logical.KleeneExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class KleeneExpressionToFeatureConverter extends ExpressionToFeatureConverter<KleeneExpression> {

    public KleeneExpressionToFeatureConverter(final FeatureFactory factory, final AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final KleeneExpression kleeneExpression, final Context context, final FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        switch (kleeneExpression.getOperator()) {
            case AND:
                signature = FunctionSignature.AND;
                break;
            case OR:
                signature = FunctionSignature.OR;
                break;
            case XOR:
                signature = FunctionSignature.XOR;
                break;
            case IMPLIES:
                signature = FunctionSignature.IMPLIES;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported boolean operation");
        }

        final Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.LEFT)
                        .withParameterValue(factory.convert(kleeneExpression.getLeft(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.RIGHT)
                        .withParameterValue(factory.convert(kleeneExpression.getRight(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
