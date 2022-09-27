package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.TimestampAsMillisecondsExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class TimestampAsMillisecondsExpressionToFeatureConverter extends ExpressionToFeatureConverter<TimestampAsMillisecondsExpression> {

    public TimestampAsMillisecondsExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(TimestampAsMillisecondsExpression expression, Context context, FeatureTargetMapping targetMapping) {
        Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.TIMESTAMP_AS_MILLISECONDS)
                .withParameters(newFunctionParameterBuilder()
                                        .withParameterName(ParameterName.TIMESTAMP)
                                        .withParameterValue(factory.convert(expression.getTimestamp(), context, null))
                                        .build())
                .build();
        context.addFeature(feature);
        return feature;
    }

}
