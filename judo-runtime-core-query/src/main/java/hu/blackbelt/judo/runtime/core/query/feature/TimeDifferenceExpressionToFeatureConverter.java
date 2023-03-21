package hu.blackbelt.judo.runtime.core.query.feature;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.temporal.TimeDifferenceExpression;
import hu.blackbelt.judo.meta.measure.DurationType;
import hu.blackbelt.judo.meta.measure.DurationUnit;
import hu.blackbelt.judo.meta.measure.Measure;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.meta.query.FunctionSignature;
import hu.blackbelt.judo.meta.query.ParameterName;
import hu.blackbelt.judo.runtime.core.query.Constants;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

public class TimeDifferenceExpressionToFeatureConverter extends ExpressionToFeatureConverter<TimeDifferenceExpression> {

    public TimeDifferenceExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final TimeDifferenceExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
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
                rate = new BigDecimal("1000000").multiply(durationUnit.get().getRateDividend()).divide(durationUnit.get().getRateDivisor(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case MICROSECOND:
                rate = new BigDecimal("1000").multiply(durationUnit.get().getRateDividend()).divide(durationUnit.get().getRateDivisor(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case MILLISECOND:
                rate = durationUnit.get().getRateDividend().divide(durationUnit.get().getRateDivisor(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case SECOND:
                rate = durationUnit.get().getRateDividend().divide(new BigDecimal("1000").multiply(durationUnit.get().getRateDivisor()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case MINUTE:
                rate = durationUnit.get().getRateDividend().divide(new BigDecimal("60000").multiply(durationUnit.get().getRateDivisor()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case HOUR:
                rate = durationUnit.get().getRateDividend().divide(new BigDecimal("3600000").multiply(durationUnit.get().getRateDivisor()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case DAY:
                rate = durationUnit.get().getRateDividend().divide(new BigDecimal("86400000").multiply(durationUnit.get().getRateDivisor()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case WEEK:
                rate = durationUnit.get().getRateDividend().divide(new BigDecimal("604800000").multiply(durationUnit.get().getRateDivisor()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            default:
                throw new IllegalStateException("Could not calculate rate of duration unit for addition of date");
        }

        final Feature difference = newFunctionBuilder()
                .withSignature(FunctionSignature.DIFFERENCE_TIME)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.END)
                        .withParameterValue(factory.convert(expression.getEndTime(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.START)
                        .withParameterValue(factory.convert(expression.getStartTime(), context, null))
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
