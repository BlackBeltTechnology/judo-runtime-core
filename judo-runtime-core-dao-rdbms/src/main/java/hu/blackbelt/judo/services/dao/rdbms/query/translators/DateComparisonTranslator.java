package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.DateExpression;
import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.logical.DateComparison;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newDateComparisonBuilder;

@Builder
public class DateComparisonTranslator implements Function<DateComparison, DateComparison> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public DateComparison apply(final DateComparison dateComparator) {
        return newDateComparisonBuilder()
                .withLeft((DateExpression) translator.apply(dateComparator.getLeft()))
                .withOperator(dateComparator.getOperator())
                .withRight((DateExpression) translator.apply(dateComparator.getRight()))
                .build();
    }
}
