package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.StringExpression;
import hu.blackbelt.judo.meta.expression.logical.StringComparison;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newStringComparisonBuilder;

@Builder
public class StringComparisonTranslator implements Function<StringComparison, StringComparison> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public StringComparison apply(final StringComparison stringComparator) {
        return newStringComparisonBuilder()
                .withLeft((StringExpression) translator.apply(stringComparator.getLeft()))
                .withOperator(stringComparator.getOperator())
                .withRight((StringExpression) translator.apply(stringComparator.getRight()))
                .build();
    }
}
