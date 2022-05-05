package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.EnumerationExpression;
import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.logical.EnumerationComparison;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newEnumerationComparisonBuilder;

@Builder
public class EnumerationComparisonTranslator implements Function<EnumerationComparison, EnumerationComparison> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public EnumerationComparison apply(final EnumerationComparison enumerationComparator) {
        return newEnumerationComparisonBuilder()
                .withLeft((EnumerationExpression) translator.apply(enumerationComparator.getLeft()))
                .withOperator(enumerationComparator.getOperator())
                .withRight((EnumerationExpression) translator.apply(enumerationComparator.getRight()))
                .build();
    }
}
