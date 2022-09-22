package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.TimestampConversion;
import hu.blackbelt.judo.meta.expression.numeric.TimestampConversionExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class TimestampConversionExpressionToFeatureConverter extends ExpressionToFeatureConverter<TimestampConversionExpression> {

    public TimestampConversionExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(TimestampConversionExpression expression, Context context, FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        if (expression.getTimestampConversion() == TimestampConversion.MILLISEC) {
            signature = FunctionSignature.TIMESTAMP_AS_MILLISECONDS;
        } else {
            throw new IllegalArgumentException("Timestamp conversion not supported: " + expression.getTimestampConversion().getName());
        }

        Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                                        .withParameterName(ParameterName.TIMESTAMP)
                                        .withParameterValue(factory.convert(expression.getTimestamp(), context, null))
                                        .build())
                .build();
        context.addFeature(feature);
        return feature;
    }

}
