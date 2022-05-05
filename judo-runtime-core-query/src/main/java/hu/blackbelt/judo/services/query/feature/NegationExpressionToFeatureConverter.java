package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.logical.NegationExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class NegationExpressionToFeatureConverter extends ExpressionToFeatureConverter<NegationExpression> {

    public NegationExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final NegationExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.NOT)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.BOOLEAN)
                        .withParameterValue(factory.convert(expression.getExpression(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
