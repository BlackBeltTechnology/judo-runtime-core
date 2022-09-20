package hu.blackbelt.judo.runtime.core.query;

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

import hu.blackbelt.judo.meta.expression.AttributeSelector;
import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.SwitchExpression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.adapters.measure.MeasureProvider;
import hu.blackbelt.judo.meta.expression.collection.CastCollection;
import hu.blackbelt.judo.meta.expression.constant.Constant;
import hu.blackbelt.judo.meta.expression.logical.*;
import hu.blackbelt.judo.meta.expression.numeric.*;
import hu.blackbelt.judo.meta.expression.object.*;
import hu.blackbelt.judo.meta.expression.string.*;
import hu.blackbelt.judo.meta.expression.temporal.*;
import hu.blackbelt.judo.meta.expression.variable.EnvironmentVariable;
import hu.blackbelt.judo.meta.measure.Measure;
import hu.blackbelt.judo.meta.measure.Unit;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.runtime.core.query.feature.*;
import hu.blackbelt.judo.runtime.core.query.feature.aggregated.*;
import hu.blackbelt.mapper.api.Coercer;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class FeatureFactory {

    @SuppressWarnings("rawtypes")
	private Map<Class, ExpressionToFeatureConverter> converters = new ConcurrentHashMap<>();

    public FeatureFactory(final JoinFactory joinFactory, final AsmModelAdapter modelAdapter, final Coercer coercer, final MeasureProvider<Measure, Unit> measureProvider) {
        converters.put(AsString.class, new AsStringToFeatureConverter(this, modelAdapter));
        converters.put(AttributeSelector.class, new AttributeToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(ObjectSelectorExpression.class, new ObjectSelectorExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(ObjectNavigationExpression.class, new ObjectNavigationToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(Capitalize.class, new CapitalizeToFeatureConverter(this, modelAdapter));
        converters.put(CastObject.class, new CastObjectToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(CastCollection.class, new CastCollectionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(ContainerExpression.class, new ContainerExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(Concatenate.class, new ConcatenateToFeatureConverter(this, modelAdapter));
        converters.put(ConcatenateCollection.class, new ConcatenateCollectionToFeatureConverter(this, modelAdapter));
        converters.put(Constant.class, new ConstantToFeatureConverter(this, coercer, modelAdapter, measureProvider));
        converters.put(ContainsExpression.class, new ContainsExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(CountExpression.class, new CountExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(DateAdditionExpression.class, new DateAdditionExpressionToFeatureConverter(this, modelAdapter));
        converters.put(DateAggregatedExpression.class, new DateAggregatedExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(DateComparison.class, new DateComparisonToFeatureConverter(this, modelAdapter));
        converters.put(DateConstructionExpression.class, new DateConstructionExpressionToFeatureConverter(this, modelAdapter));
        converters.put(DateDifferenceExpression.class, new DateDifferenceExpressionToFeatureConverter(this, modelAdapter));
        converters.put(DecimalAggregatedExpression.class, new DecimalAggregatedExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(DecimalArithmeticExpression.class, new DecimalArithmeticExpressionToFeatureConverter(this, modelAdapter));
        converters.put(DecimalComparison.class, new DecimalComparisonToFeatureConverter(this, modelAdapter));
        converters.put(DecimalOppositeExpression.class, new DecimalOppositeToFeatureConverter(this, modelAdapter));
        converters.put(EnumerationComparison.class, new EnumerationComparisonToFeatureConverter(this, modelAdapter));
        converters.put(EnvironmentVariable.class, new EnvironmentVariableToFeatureConverter(this, modelAdapter));
        converters.put(Empty.class, new EmptyToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(Exists.class, new ExistsToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(ExtractDateExpression.class, new ExtractDateExpressionToFeatureConverter(this, modelAdapter));
        converters.put(ExtractTimeExpression.class, new ExtractTimeExpressionToFeatureConverter(this, modelAdapter));
        converters.put(ExtractTimestampExpression.class, new ExtractTimestampExpressionToFeatureConverter(this, modelAdapter));
        converters.put(ForAll.class, new ForAllToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(InstanceOfExpression.class, new InstanceOfExpressionToFeatureConverter(this, modelAdapter));
        converters.put(IntegerAggregatedExpression.class, new IntegerAggregatedExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(IntegerArithmeticExpression.class, new IntegerArithmeticExpressionToFeatureConverter(this, modelAdapter));
        converters.put(IntegerComparison.class, new IntegerComparisonToFeatureConverter(this, modelAdapter));
        converters.put(IntegerOppositeExpression.class, new IntegerOppositeToFeatureConverter(this, modelAdapter));
        converters.put(KleeneExpression.class, new KleeneExpressionToFeatureConverter(this, modelAdapter));
        converters.put(Length.class, new LengthToFeatureConverter(this, modelAdapter));
        converters.put(Like.class, new LikeToFeatureConverter(this, modelAdapter));
        converters.put(LowerCase.class, new LowerCaseToFeatureConverter(this, modelAdapter));
        converters.put(Matches.class, new MatchesToFeatureConverter(this, modelAdapter));
        converters.put(MemberOfExpression.class, new MemberOfExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(NegationExpression.class, new NegationExpressionToFeatureConverter(this, modelAdapter));
        converters.put(ObjectComparison.class, new ObjectComparisonToFeatureConverter(this, modelAdapter));
        converters.put(ObjectVariableReference.class, new ObjectVariableReferenceToFeatureConverter(this, modelAdapter));
        converters.put(Position.class, new PositionToFeatureConverter(this, modelAdapter));
        converters.put(Replace.class, new ReplaceToFeatureConverter(this, modelAdapter));
        converters.put(RoundExpression.class, new RoundToFeatureConverter(this, modelAdapter));
        converters.put(AbsoluteExpression.class, new AbsoluteToFeatureConverter(this, modelAdapter));
        converters.put(CeilExpression.class, new CeilToFeatureConverter(this, modelAdapter));
        converters.put(FloorExpression.class, new FloorToFeatureConverter(this, modelAdapter));
        converters.put(SequenceExpression.class, new SequenceExpressionToFeatureConverter(this, modelAdapter));
        converters.put(StringAggregatedExpression.class, new StringAggregatedExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(StringComparison.class, new StringComparisonToFeatureConverter(this, modelAdapter));
        converters.put(SubString.class, new SubStringToFeatureConverter(this, modelAdapter));
        converters.put(SwitchExpression.class, new SwitchExpressionToFeatureConverter(this, modelAdapter));
        converters.put(TimeAdditionExpression.class, new TimeAdditionExpressionToFeatureConverter(this, modelAdapter));
        converters.put(TimeAggregatedExpression.class, new TimeAggregatedExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(TimeComparison.class, new TimeComparisonToFeatureConverter(this, modelAdapter));
        converters.put(TimeConstructionExpression.class, new TimeConstructionExpressionToFeatureConverter(this, modelAdapter));
        converters.put(TimeDifferenceExpression.class, new TimeDifferenceExpressionToFeatureConverter(this, modelAdapter));
        converters.put(TimestampAdditionExpression.class, new TimestampAdditionExpressionToFeatureConverter(this, modelAdapter));
        converters.put(TimestampAggregatedExpression.class, new TimestampAggregatedExpressionToFeatureConverter(this, joinFactory, modelAdapter));
        converters.put(TimestampComparison.class, new TimestampComparisonToFeatureConverter(this, modelAdapter));
        converters.put(TimestampConstructionExpression.class, new TimestampConstructionExpressionToFeatureConverter(this, modelAdapter));
        converters.put(TimestampConversionExpression.class, new TimestampConversionExpressionToFeatureConverter(this, modelAdapter));
        converters.put(TimestampFromMillisecondsExpression.class, new TimestampFromMillisecondsExpressionToFeatureConverter(this, modelAdapter));
        converters.put(TimestampDifferenceExpression.class, new TimestampDifferenceExpressionToFeatureConverter(this, modelAdapter));
        converters.put(Trim.class, new TrimToFeatureConverter(this, modelAdapter));
        converters.put(TypeOfExpression.class, new TypeOfExpressionToFeatureConverter(this, modelAdapter));
        converters.put(UndefinedAttributeComparison.class, new UndefinedAttributeComparisonToFeatureConverter(this, modelAdapter));
        converters.put(UndefinedEnvironmentVariableComparison.class, new UndefinedEnvironmentVariableComparisonToFeatureConverter(this, modelAdapter));
        converters.put(UndefinedNavigationComparison.class, new UndefinedNavigationComparisonToFeatureConverter(this, modelAdapter));
        converters.put(UpperCase.class, new UpperCaseToFeatureConverter(this, modelAdapter));
    }

    public Feature convert(final Expression expression, final Context context, final FeatureTargetMapping targetMapping) {
        @SuppressWarnings({ "rawtypes", "unchecked" })
		final Optional<ExpressionToFeatureConverter> converter = converters.entrySet().stream()
                .filter(c -> c.getKey().isAssignableFrom(expression.getClass()))
                .map(c -> c.getValue())
                .findAny();

        if (!converter.isPresent()) {
            throw new IllegalStateException("Unsupported expression: " + expression.getClass().getName());
        }

        @SuppressWarnings({ "unchecked" })
		final Feature feature = converter.get().convert(expression, context, targetMapping);
        if (targetMapping != null) {
            feature.getTargetMappings().add(targetMapping);
        }
        return feature;
    }
}
