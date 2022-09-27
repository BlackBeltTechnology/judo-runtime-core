package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.temporal.TimestampFromMillisecondsExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.util.builder.QueryBuilders;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;

public class TimestampFromMillisecondsExpressionToFeatureConverter extends ExpressionToFeatureConverter<TimestampFromMillisecondsExpression> {

    public TimestampFromMillisecondsExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(TimestampFromMillisecondsExpression expression, Context context, FeatureTargetMapping targetMapping) {
        Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.TIMESTAMP_FROM_MILLISECONDS)
                .withParameters(QueryBuilders.newFunctionParameterBuilder()
                                             .withParameterName(ParameterName.NUMBER)
                                             .withParameterValue(factory.convert(expression.getMilliseconds(), context, null))
                                             .build())
                .build();
        context.addFeature(feature);
        return feature;
    }

}
