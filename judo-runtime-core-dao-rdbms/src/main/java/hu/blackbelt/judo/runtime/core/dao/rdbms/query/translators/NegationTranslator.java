package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.LogicalExpression;
import hu.blackbelt.judo.meta.expression.logical.NegationExpression;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newNegationExpressionBuilder;

@Builder
public class NegationTranslator implements Function<NegationExpression, NegationExpression> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public NegationExpression apply(final NegationExpression expression) {
        return newNegationExpressionBuilder()
                .withExpression((LogicalExpression) translator.apply(expression.getExpression()))
                .build();
    }
}
