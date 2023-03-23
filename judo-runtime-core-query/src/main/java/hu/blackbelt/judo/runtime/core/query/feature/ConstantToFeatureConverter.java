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

import hu.blackbelt.judo.meta.expression.MeasureName;
import hu.blackbelt.judo.meta.expression.TypeName;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.adapters.measure.MeasureProvider;
import hu.blackbelt.judo.meta.expression.constant.*;
import hu.blackbelt.judo.meta.measure.Measure;
import hu.blackbelt.judo.meta.measure.Unit;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.runtime.core.query.Constants;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;
import hu.blackbelt.mapper.api.Coercer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EEnum;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newConstantBuilder;

@Slf4j
public class ConstantToFeatureConverter extends ExpressionToFeatureConverter<Constant> {

    @SuppressWarnings("unused")
    private final Coercer coercer;
    private final MeasureProvider<Measure, Unit> measureProvider;

    public ConstantToFeatureConverter(final FeatureFactory factory, final Coercer coercer, final AsmModelAdapter modelAdapter, final MeasureProvider<Measure, Unit> measureProvider) {
        super(factory, modelAdapter);
        this.coercer = coercer;
        this.measureProvider = measureProvider;
    }

    @Override
    public Feature convert(final Constant constant, final Context context, final FeatureTargetMapping targetMapping) {
        final Object value;
        if (constant instanceof MeasuredDecimal) {
            final String unitName;
            final MeasureName measureName;
            final BigDecimal number;
            unitName = ((MeasuredDecimal) constant).getUnitName();
            measureName = ((MeasuredDecimal) constant).getMeasure();
            number = ((MeasuredDecimal) constant).getValue();

            final Optional<Measure> measure = Optional.ofNullable(measureName != null ? measureProvider.getMeasure(measureName.getNamespace(), measureName.getName()).get() : null);
            final Optional<Unit> unit = measureProvider.getUnitByNameOrSymbol(measure, unitName);

            if (!unit.isPresent()) {
                throw new IllegalStateException("Unit not found: " + unitName);
            }

            final BigDecimal dividend = unit.get().getRateDividend();
            final BigDecimal divisor = unit.get().getRateDivisor();

            final BigDecimal decimal;
            final Optional<Unit> targetUnit = targetMapping != null ? modelAdapter.getUnit(targetMapping.getTargetAttribute()) : Optional.empty();
            if (targetUnit.isPresent()) {
                decimal = dividend.multiply(targetUnit.get().getRateDivisor()).multiply(number).divide(divisor, Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP).divide(targetUnit.get().getRateDividend(), Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
            } else {
                decimal = dividend.multiply(number).divide(divisor, Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP);
            }

            value = decimal;
            if (log.isTraceEnabled()) {
                log.trace("Constant {} is converted to {}", constant, value);
            }
        } else if (constant instanceof IntegerConstant) {
            value = ((IntegerConstant) constant).getValue();
        } else if (constant instanceof DecimalConstant) {
            value = ((DecimalConstant) constant).getValue();
        } else if (constant instanceof BooleanConstant) {
            value = ((BooleanConstant) constant).isValue();
        } else if (constant instanceof StringConstant) {
            value = ((StringConstant) constant).getValue();
        } else if (constant instanceof Literal) {
            final String literal = ((Literal) constant).getValue();
            final TypeName enumName = ((Literal) constant).getEnumeration();
            final Optional<EEnum> enumeration = modelAdapter.get(enumName)
                    .filter(e -> e instanceof EEnum)
                    .map(e -> (EEnum) e);
            checkArgument(enumeration != null, "Enumeration not found");
            value = enumeration.get().getEEnumLiteral(literal).getValue();
        } else if (constant instanceof DateConstant) {
            value = ((DateConstant) constant).getValue();
        } else if (constant instanceof TimestampConstant) {
            value = ((TimestampConstant) constant).getValue();
        } else if (constant instanceof TimeConstant) {
            value = ((TimeConstant) constant).getValue();
        } else if (constant instanceof CustomData) {
            value = ((CustomData) constant).getValue();
        } else {
            throw new UnsupportedOperationException("Invalid constant: " + constant);
        }

        Feature feature = newConstantBuilder()
                .withValue(value)
                .build();

        context.addFeature(feature);
        return feature;
    }
}
