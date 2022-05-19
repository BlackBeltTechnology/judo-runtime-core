package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.temporal.DateDifferenceExpression;
import hu.blackbelt.judo.meta.measure.DurationType;
import hu.blackbelt.judo.meta.measure.DurationUnit;
import hu.blackbelt.judo.meta.measure.Measure;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Constants;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

public class DateDifferenceExpressionToFeatureConverter extends ExpressionToFeatureConverter<DateDifferenceExpression> {

    public DateDifferenceExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final DateDifferenceExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        Optional<Measure> measure = modelAdapter.getAllMeasures().stream()
                .filter(m -> AsmUtils.equals(expression.getMeasure(), modelAdapter.buildMeasureName(m).orElse(null)))
                .findAny();

        if (!measure.isPresent()) {
            measure = modelAdapter.getAllMeasures().stream()
                    .filter(m -> m.getUnits().stream().anyMatch(u -> (u instanceof DurationUnit) && ((DurationUnit) u).getType() != DurationType.YEAR && ((DurationUnit) u).getType() != DurationType.MONTH))
                    .findAny();
        }

        checkArgument(measure.isPresent(), "Unknown measure to calculate elapsed time");

        final Optional<DurationUnit> durationUnit = measure.get().getUnits().stream()
                .filter(u -> u instanceof DurationUnit).map(u -> (DurationUnit) u)
                .filter(u -> u.getType() != DurationType.YEAR && u.getType() != DurationType.MONTH) // calculating elapsed time in months and years is not allowed
                .findAny();

        checkArgument(durationUnit.isPresent(), "Not duration unit found to calculate elapse time");

        final BigDecimal rate;
        switch (durationUnit.get().getType()) {
            case NANOSECOND:
                rate = new BigDecimal("86400000000000").multiply(durationUnit.get().getRateDividend()).divide(durationUnit.get().getRateDivisor(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case MICROSECOND:
                rate = new BigDecimal("86400000000").multiply(durationUnit.get().getRateDividend()).divide(durationUnit.get().getRateDivisor(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case MILLISECOND:
                rate = new BigDecimal("86400000").multiply(durationUnit.get().getRateDividend()).divide(durationUnit.get().getRateDivisor(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case SECOND:
                rate = new BigDecimal("86400").multiply(durationUnit.get().getRateDividend()).divide(durationUnit.get().getRateDivisor(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case MINUTE:
                rate = new BigDecimal("1440").multiply(durationUnit.get().getRateDividend()).divide(durationUnit.get().getRateDivisor(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case HOUR:
                rate = new BigDecimal("24").multiply(durationUnit.get().getRateDividend()).divide(durationUnit.get().getRateDivisor(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case DAY:
                rate = durationUnit.get().getRateDividend().divide(durationUnit.get().getRateDivisor(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case WEEK:
                rate = durationUnit.get().getRateDividend().divide(new BigDecimal("7").multiply(durationUnit.get().getRateDivisor()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            default:
                throw new IllegalStateException("Could not calculate rate of duration unit for addition of date");
        }

        final Feature difference = newFunctionBuilder()
                .withSignature(FunctionSignature.DIFFERENCE_DATE)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.END)
                        .withParameterValue(factory.convert(expression.getEndDate(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.START)
                        .withParameterValue(factory.convert(expression.getStartDate(), context, null))
                        .build())
                .build();
        context.addFeature(difference);

        final Feature scaledDifference;
        if (BigDecimal.ONE.equals(rate)) {
            scaledDifference = difference;
        } else {
            final Feature rateConstant = newConstantBuilder()
                    .withValue(rate)
                    .build();
            context.addFeature(rateConstant);

            scaledDifference = newFunctionBuilder()
                    .withSignature(FunctionSignature.MULTIPLE_DECIMAL)
                    .withParameters(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.LEFT)
                            .withParameterValue(difference)
                            .build())
                    .withParameters(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.RIGHT)
                            .withParameterValue(rateConstant)
                            .build())
                    .build();
            context.addFeature(scaledDifference);
        }

        return applyMeasure(context, scaledDifference, null, targetMapping);
    }
}
