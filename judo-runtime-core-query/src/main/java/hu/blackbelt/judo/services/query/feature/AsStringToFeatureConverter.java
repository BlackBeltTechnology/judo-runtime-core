package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.*;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.constant.MeasuredDecimal;
import hu.blackbelt.judo.meta.expression.string.AsString;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EEnumLiteral;

import java.math.BigInteger;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.meta.expression.constant.util.builder.ConstantBuilders.newIntegerConstantBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

public class AsStringToFeatureConverter extends ExpressionToFeatureConverter<AsString> {

    public AsStringToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final AsString expression, final Context context, final FeatureTargetMapping targetMapping) {
        if (expression.getExpression() instanceof StringExpression) {
            // do not cast String
            return factory.convert(expression.getExpression(), context, null);
        } else if (expression.getExpression() instanceof EnumerationExpression) {
            final TypeName typeName = ((EnumerationExpression) expression.getExpression()).getEnumeration(modelAdapter);
            checkState(typeName != null, "Unknown enumeration");
            final EEnum enumerationType = modelAdapter.get(typeName)
                    .filter(t -> t instanceof EEnum)
                    .map(t -> (EEnum) t)
                    .orElseThrow(() -> new IllegalStateException("Unable to result enumeration: " + typeName));

            Feature feature = null;
            Function lastFeature = null;
            for (final Iterator<EEnumLiteral> it = enumerationType.getELiterals().iterator(); it.hasNext(); ) {
                final EEnumLiteral literal = it.next();

                final Feature condition = newFunctionBuilder()
                        .withSignature(FunctionSignature.EQUALS)
                        .withParameters(newFunctionParameterBuilder()
                                .withParameterName(ParameterName.LEFT)
                                .withParameterValue(factory.convert(expression.getExpression(), context, null))
                                .build())
                        .withParameters(newFunctionParameterBuilder()
                                .withParameterName(ParameterName.RIGHT)
                                .withParameterValue(factory.convert(newIntegerConstantBuilder().withValue(BigInteger.valueOf(literal.getValue())).build(), context, null))
                                .build())
                        .build();
                final Feature value = newConstantBuilder().withValue(literal.getName()).build();
                context.addFeature(condition);
                context.addFeature(value);

                final Function function = newFunctionBuilder()
                        .withSignature(FunctionSignature.CASE_WHEN)
                        .withParameters(newFunctionParameterBuilder()
                                .withParameterName(ParameterName.CONDITION)
                                .withParameterValue(condition)
                                .build())
                        .withParameters(newFunctionParameterBuilder()
                                .withParameterName(ParameterName.LEFT)
                                .withParameterValue(value)
                                .build())
                        .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                        .build();
                context.addFeature(function);

                if (feature == null) {
                    feature = function;
                } else {
                    lastFeature.getParameters().add(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.RIGHT)
                            .withParameterValue(function)
                            .build());
                }
                if (!it.hasNext()) {
                    final Feature undefined = newFunctionBuilder().withSignature(FunctionSignature.UNDEFINED).build();
                    function.getParameters().add(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.RIGHT)
                            .withParameterValue(undefined)
                            .build());
                    context.addFeature(undefined);
                }
                lastFeature = function;
            }

            return feature;
        } else {
            final FunctionSignature signature;
            if (expression.getExpression() instanceof IntegerExpression) {
                signature = FunctionSignature.INTEGER_TO_STRING;
            } else if (expression.getExpression() instanceof DecimalExpression) {
                signature = FunctionSignature.DECIMAL_TO_STRING;
            } else if (expression.getExpression() instanceof DateExpression) {
                signature = FunctionSignature.DATE_TO_STRING;
            } else if (expression.getExpression() instanceof TimestampExpression) {
                signature = FunctionSignature.TIMESTAMP_TO_STRING;
            } else if (expression.getExpression() instanceof TimeExpression) {
                signature = FunctionSignature.TIME_TO_STRING;
            } else if (expression.getExpression() instanceof LogicalExpression) {
                signature = FunctionSignature.LOGICAL_TO_STRING;
            } else if (expression.getExpression() instanceof EnumerationExpression) {
                signature = FunctionSignature.ENUM_TO_STRING;
            } else {
                signature = FunctionSignature.CUSTOM_TO_STRING;
            }

            final Feature feature = newFunctionBuilder()
                    .withSignature(signature)
                    .withParameters(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.PRIMITIVE)
                            .withParameterValue(factory.convert(expression.getExpression(), context, null))
                            .build())
                    .withParameters(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.TYPE)
                            .withParameterValue(factory.convert(expression.getExpression(), context, null))
                            .build())
                    .build();

            context.addFeature(feature);
            return feature;
        }
    }
}
