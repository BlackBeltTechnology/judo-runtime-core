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

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.temporal.DateAdditionExpression;
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

public class DateAdditionExpressionToFeatureConverter extends ExpressionToFeatureConverter<DateAdditionExpression> {

    public DateAdditionExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final DateAdditionExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Optional<Measure> measure = modelAdapter.getMeasure(expression.getDuration());
        checkArgument(measure.isPresent(), "Unknown measure of addition");

        final Optional<DurationUnit> durationUnit = measure.get().getUnits().stream()
                .filter(u -> u instanceof DurationUnit).map(u -> (DurationUnit) u)
                .filter(u -> u.getType() != DurationType.YEAR && u.getType() != DurationType.MONTH) // addition of months and years is not allowed
                .findAny();

        checkArgument(durationUnit.isPresent(), "Not duration unit found for addition of date");

        final BigDecimal rate;
        switch (durationUnit.get().getType()) {
            case NANOSECOND:
                rate = durationUnit.get().getRateDivisor().divide(new BigDecimal("86400000000000").multiply(durationUnit.get().getRateDividend()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case MICROSECOND:
                rate = durationUnit.get().getRateDivisor().divide(new BigDecimal("86400000000").multiply(durationUnit.get().getRateDividend()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case MILLISECOND:
                rate = durationUnit.get().getRateDivisor().divide(new BigDecimal("86400000").multiply(durationUnit.get().getRateDividend()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case SECOND:
                rate = durationUnit.get().getRateDivisor().divide(new BigDecimal("86400").multiply(durationUnit.get().getRateDividend()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case MINUTE:
                rate = durationUnit.get().getRateDivisor().divide(new BigDecimal("1440").multiply(durationUnit.get().getRateDividend()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case HOUR:
                rate = durationUnit.get().getRateDivisor().divide(new BigDecimal("24").multiply(durationUnit.get().getRateDividend()), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case DAY:
                rate = durationUnit.get().getRateDivisor().divide(durationUnit.get().getRateDividend(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            case WEEK:
                rate = new BigDecimal("7").multiply(durationUnit.get().getRateDivisor()).divide(durationUnit.get().getRateDividend(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
                break;
            default:
                throw new IllegalStateException("Could not calculate rate of duration unit for addition of date");
        }

        final Feature addition = factory.convert(expression.getDuration(), context, null);
        final Feature scaledAddition;
        if (BigDecimal.ONE.equals(rate)) {
            scaledAddition = addition;
        } else {
            final Feature rateConstant = newConstantBuilder()
                    .withValue(rate)
                    .build();
            context.addFeature(rateConstant);

            scaledAddition = newFunctionBuilder()
                    .withSignature(FunctionSignature.MULTIPLE_DECIMAL)
                    .withParameters(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.LEFT)
                            .withParameterValue(addition)
                            .build())
                    .withParameters(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.RIGHT)
                            .withParameterValue(rateConstant)
                            .build())
                    .withConstraints(newFunctionConstraintBuilder()
                            .withResultConstraint(ResultConstraint.PRECISION)
                            .withValue(String.valueOf(Constants.MEASURE_CONVERTING_PRECISION))
                            .build())
                    .withConstraints(newFunctionConstraintBuilder()
                            .withResultConstraint(ResultConstraint.SCALE)
                            .withValue(String.valueOf(Constants.MEASURE_CONVERTING_SCALE))
                            .build())
                    .build();
            context.addFeature(scaledAddition);
        }

        final Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.ADD_DATE)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.DATE)
                        .withParameterValue(factory.convert(expression.getExpression(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.ADDITION)
                        .withParameterValue(scaledAddition)
                        .build())
                .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                .build();

        context.addFeature(feature);
        return feature;
    }
}
