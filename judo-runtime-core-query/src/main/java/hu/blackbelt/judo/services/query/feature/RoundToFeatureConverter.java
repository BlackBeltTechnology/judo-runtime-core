package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.RoundExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.util.builder.QueryBuilders;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;

public class RoundToFeatureConverter extends ExpressionToFeatureConverter<RoundExpression> {

    public RoundToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final RoundExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Feature feature = QueryBuilders.newFunctionBuilder()
                .withSignature(FunctionSignature.ROUND_DECIMAL)
                .withParameters(QueryBuilders.newFunctionParameterBuilder()
                        .withParameterName(ParameterName.NUMBER)
                        .withParameterValue(factory.convert(expression.getExpression(), context, null))
                        .build())
                .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                .build();

        context.addFeature(feature);
        return feature;
    }
}
