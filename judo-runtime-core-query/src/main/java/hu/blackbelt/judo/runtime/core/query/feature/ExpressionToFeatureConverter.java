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
import hu.blackbelt.judo.meta.expression.CollectionExpression;
import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.ObjectExpression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.constant.Instance;
import hu.blackbelt.judo.meta.expression.object.ObjectNavigationExpression;
import hu.blackbelt.judo.meta.expression.object.ObjectSelectorExpression;
import hu.blackbelt.judo.meta.expression.object.ObjectVariableReference;
import hu.blackbelt.judo.meta.expression.variable.ObjectVariable;
import hu.blackbelt.judo.meta.measure.Unit;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Constants;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EAttribute;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import java.util.Optional;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

@Slf4j
public abstract class ExpressionToFeatureConverter<E extends Expression> {

    private static final String ITERATOR_NAME = "_iterator";

    protected final FeatureFactory factory;
    protected final AsmModelAdapter modelAdapter;

    public ExpressionToFeatureConverter(FeatureFactory factory, final AsmModelAdapter modelAdapter) {
        this.factory = factory;
        this.modelAdapter = modelAdapter;
    }

    public abstract Feature convert(E expression, Context context, FeatureTargetMapping targetMapping);

    protected Feature applyMeasure(final Context context, final Feature feature, final EAttribute entityAttribute, final FeatureTargetMapping targetMapping) {
        return applyMeasureByUnit(context, feature, entityAttribute != null ? modelAdapter.getUnit(entityAttribute) : Optional.empty(), targetMapping);
    }

    protected Feature applyMeasureByUnit(final Context context, final Feature feature, final Optional<Unit> entityUnit, final FeatureTargetMapping targetMapping) {
        final Optional<Unit> targetUnit = targetMapping != null ? modelAdapter.getUnit(targetMapping.getTargetAttribute()) : Optional.empty();

        if (entityUnit.isPresent() || targetUnit.isPresent()) {
            final BigDecimal dividend = targetUnit.map(u -> u.getRateDividend()).orElse(BigDecimal.ONE).multiply(entityUnit.map(u -> u.getRateDivisor()).orElse(BigDecimal.ONE));
            final BigDecimal divisor = targetUnit.map(u -> u.getRateDivisor()).orElse(BigDecimal.ONE).multiply(entityUnit.map(u -> u.getRateDividend()).orElse(BigDecimal.ONE));

            if (log.isDebugEnabled()) {
                log.debug("Applying measure rate on {}: {}/{}", new Object[]{feature, dividend, divisor});
            }

            if (Objects.equals(dividend, divisor)) {
                return feature;
            } else {
                final Feature rateConstant = newConstantBuilder()
                        .withValue(divisor.divide(dividend, Constants.MEASURE_RATE_CALCULATION_SCALE, RoundingMode.HALF_UP))
                        .build();
                context.addFeature(rateConstant);

                final Feature function = newFunctionBuilder()
                        .withSignature(FunctionSignature.MULTIPLE_DECIMAL)
                        .withParameters(newFunctionParameterBuilder()
                                .withParameterName(ParameterName.LEFT)
                                .withParameterValue(feature).build())
                        .withParameters(newFunctionParameterBuilder()
                                .withParameterName(ParameterName.RIGHT)
                                .withParameterValue(rateConstant)
                                .build())
                        .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                        .build();
                context.addFeature(function);
                return function;
            }
        } else {
            return feature;
        }
    }

    protected EList<FunctionConstraint> getConstraints(final EAttribute attribute) {
        if (attribute == null) {
            return ECollections.emptyEList();
        } else {
            final Optional<String> precision = AsmUtils.getExtensionAnnotationCustomValue(attribute, "constraints", "precision", false);
            final Optional<String> scale = AsmUtils.getExtensionAnnotationCustomValue(attribute, "constraints", "scale", false);
            final Optional<String> maxLength = AsmUtils.getExtensionAnnotationCustomValue(attribute, "constraints", "maxLength", false);

            final EList<FunctionConstraint> constraints = ECollections.newBasicEList();
            if (precision.isPresent()) {
                constraints.add(newFunctionConstraintBuilder().withResultConstraint(ResultConstraint.PRECISION).withValue(precision.get()).build());
            }
            if (scale.isPresent()) {
                constraints.add(newFunctionConstraintBuilder().withResultConstraint(ResultConstraint.SCALE).withValue(scale.get()).build());
            }
            if (maxLength.isPresent()) {
                constraints.add(newFunctionConstraintBuilder().withResultConstraint(ResultConstraint.MAX_LENGTH).withValue(maxLength.get()).build());
            }
            return constraints;
        }
    }

    protected Node getSourceByVariableName(ObjectExpression objectExpression, final Context context) {
        final String variableName = getBaseVariableName(objectExpression);
        final Node node = context.getVariables().get(variableName);

        if (node == null) {
            log.error("Variable not found: {} in [{}]", variableName, context);
            throw new IllegalStateException("Variable not found: " + variableName);
        }

        return node;
    }

    protected String getBaseVariableName(ObjectExpression objectExpression) {
        if (objectExpression instanceof ObjectVariableReference) {
            return ((ObjectVariableReference)objectExpression).getVariableName();
        } else if (objectExpression instanceof Instance) {
            return ((Instance)objectExpression).getVariableName();
        } else if (objectExpression instanceof ObjectNavigationExpression) {
            return getBaseVariableName(((ObjectNavigationExpression) objectExpression).getObjectExpression());
        } else if (objectExpression instanceof ObjectSelectorExpression) {
            return ((ObjectSelectorExpression) objectExpression).getCollectionExpression().getIteratorVariableName();
        } else {
            throw new IllegalStateException("Not supported yet");
        }
    }

    protected ObjectVariable getCollectionIterator(CollectionExpression collectionExpression) {
        return collectionExpression.getIteratorVariable() == null ?
                collectionExpression.createIterator(ITERATOR_NAME, modelAdapter, collectionExpression.eResource()) :
                collectionExpression.getIteratorVariable();
    }
}
