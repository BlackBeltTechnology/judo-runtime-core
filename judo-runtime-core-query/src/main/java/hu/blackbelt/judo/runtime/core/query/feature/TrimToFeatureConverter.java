package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.string.Trim;
import hu.blackbelt.judo.meta.expression.string.TrimType;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.util.builder.QueryBuilders;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class TrimToFeatureConverter extends ExpressionToFeatureConverter<Trim> {

    public TrimToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final Trim expression, final Context context, final FeatureTargetMapping targetMapping) {
        TrimType trimType = expression.getTrimType();
        final FunctionSignature functionSignature;
        if (TrimType.BOTH.equals(trimType)) {
            functionSignature = FunctionSignature.TRIM_STRING;
        } else if (TrimType.LEFT.equals(trimType)) {
            functionSignature = FunctionSignature.LEFT_TRIM_STRING;
        } else if (TrimType.RIGHT.equals(trimType)) {
            functionSignature = FunctionSignature.RIGHT_TRIM_STRING;
        } else {
            throw new IllegalStateException("Trim expression's trim type contains invalid value: " + trimType);
        }
        final Feature feature = QueryBuilders.newFunctionBuilder()
                .withSignature(functionSignature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.STRING)
                        .withParameterValue(factory.convert(expression.getExpression(), context, null))
                        .build())
                .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                .build();

        context.addFeature(feature);
        return feature;
    }
}
