package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.NumericExpression;
import hu.blackbelt.judo.meta.expression.logical.DecimalComparison;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newDecimalComparisonBuilder;

@Builder
public class DecimalComparisonTranslator implements Function<DecimalComparison, DecimalComparison> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public DecimalComparison apply(final DecimalComparison decimalComparator) {
        return newDecimalComparisonBuilder()
                .withLeft((NumericExpression) translator.apply(decimalComparator.getLeft()))
                .withOperator(decimalComparator.getOperator())
                .withRight((NumericExpression) translator.apply(decimalComparator.getRight()))
                .build();
    }
}
