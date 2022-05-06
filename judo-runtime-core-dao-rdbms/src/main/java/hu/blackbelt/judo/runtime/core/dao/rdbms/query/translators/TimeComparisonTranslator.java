package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.TimeExpression;
import hu.blackbelt.judo.meta.expression.logical.TimeComparison;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newTimeComparisonBuilder;

@Builder
public class TimeComparisonTranslator implements Function<TimeComparison, TimeComparison> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public TimeComparison apply(final TimeComparison timestampComparator) {
        return newTimeComparisonBuilder()
                .withLeft((TimeExpression) translator.apply(timestampComparator.getLeft()))
                .withOperator(timestampComparator.getOperator())
                .withRight((TimeExpression) translator.apply(timestampComparator.getRight()))
                .build();
    }
}
