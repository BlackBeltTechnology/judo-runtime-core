package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

@Getter
public class Translator implements Function<Expression, Expression> {

    private final Map<Class, Function> translators = new LinkedHashMap<>();

    @Override
    public Expression apply(final Expression expression) {
        return translators.entrySet().stream()
                .filter(t -> t.getKey().isAssignableFrom(expression.getClass()))
                .findFirst()
                .map(t -> (Expression) t.getValue().apply(expression))
                .orElseThrow(() -> new IllegalStateException("Translator not found for expression: " + expression + ", type: " + expression.getClass()));
    }
}
