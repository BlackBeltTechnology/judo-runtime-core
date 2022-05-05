package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.StringExpression;
import hu.blackbelt.judo.meta.expression.logical.Matches;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newMatchesBuilder;

@Builder
public class MatchesTranslator implements Function<Matches, Matches> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public Matches apply(final Matches matchesExpression) {
        return newMatchesBuilder()
                .withExpression((StringExpression) translator.apply(matchesExpression.getExpression()))
                .withPattern((StringExpression) translator.apply(matchesExpression.getPattern()))
                .build();
    }
}
