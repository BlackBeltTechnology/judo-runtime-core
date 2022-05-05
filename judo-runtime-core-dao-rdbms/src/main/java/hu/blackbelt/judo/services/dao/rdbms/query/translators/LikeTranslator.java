package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.StringExpression;
import hu.blackbelt.judo.meta.expression.logical.Like;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newLikeBuilder;

@Builder
public class LikeTranslator implements Function<Like, Like> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public Like apply(final Like likeExpression) {
        return newLikeBuilder()
                .withExpression((StringExpression) translator.apply(likeExpression.getExpression()))
                .withPattern((StringExpression) translator.apply(likeExpression.getPattern()))
                .withCaseInsensitive(likeExpression.isCaseInsensitive())
                .build();
    }
}
