package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.LogicalExpression;
import hu.blackbelt.judo.meta.expression.logical.KleeneExpression;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newKleeneExpressionBuilder;

@Builder
public class KleeneTranslator implements Function<KleeneExpression, KleeneExpression> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public KleeneExpression apply(final KleeneExpression expression) {
        return newKleeneExpressionBuilder()
                .withLeft((LogicalExpression) translator.apply(expression.getLeft()))
                .withOperator(expression.getOperator())
                .withRight((LogicalExpression) translator.apply(expression.getRight()))
                .build();
    }
}
