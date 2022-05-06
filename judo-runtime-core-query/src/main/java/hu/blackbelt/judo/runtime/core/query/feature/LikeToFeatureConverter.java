package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.logical.Like;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.meta.query.FunctionSignature;
import hu.blackbelt.judo.meta.query.ParameterName;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class LikeToFeatureConverter extends ExpressionToFeatureConverter<Like> {

    public LikeToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final Like expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Feature feature = newFunctionBuilder()
                .withSignature(expression.isCaseInsensitive() ? FunctionSignature.ILIKE : FunctionSignature.LIKE)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.STRING)
                        .withParameterValue(factory.convert(expression.getExpression(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.PATTERN)
                        .withParameterValue(factory.convert(expression.getPattern(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
