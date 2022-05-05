package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.IntegerExpression;
import hu.blackbelt.judo.meta.expression.logical.IntegerComparison;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newIntegerComparisonBuilder;

@Builder
public class IntegerComparisonTranslator implements Function<IntegerComparison, IntegerComparison> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public IntegerComparison apply(final IntegerComparison integerComparison) {
        return newIntegerComparisonBuilder()
                .withLeft((IntegerExpression) translator.apply(integerComparison.getLeft()))
                .withOperator(integerComparison.getOperator())
                .withRight((IntegerExpression) translator.apply(integerComparison.getRight()))
                .build();
    }
}
