package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.TimestampExpression;
import hu.blackbelt.judo.meta.expression.logical.TimestampComparison;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newTimestampComparisonBuilder;

@Builder
public class TimestampComparisonTranslator implements Function<TimestampComparison, TimestampComparison> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public TimestampComparison apply(final TimestampComparison timestampComparator) {
        return newTimestampComparisonBuilder()
                .withLeft((TimestampExpression) translator.apply(timestampComparator.getLeft()))
                .withOperator(timestampComparator.getOperator())
                .withRight((TimestampExpression) translator.apply(timestampComparator.getRight()))
                .build();
    }
}
