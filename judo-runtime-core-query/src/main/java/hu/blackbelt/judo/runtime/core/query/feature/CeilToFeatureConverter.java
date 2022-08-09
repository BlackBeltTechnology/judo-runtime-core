package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.CeilExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.query.util.builder.QueryBuilders;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

public class CeilToFeatureConverter extends ExpressionToFeatureConverter<CeilExpression> {

    public CeilToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final CeilExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Feature feature = QueryBuilders.newFunctionBuilder()
                .withSignature(FunctionSignature.CEIL_NUMERIC)
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
