package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.object.ObjectVariableReference;
import hu.blackbelt.judo.meta.expression.variable.ObjectVariable;
import lombok.Builder;
import lombok.NonNull;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.expression.object.util.builder.ObjectBuilders.newObjectVariableReferenceBuilder;

@Builder
public class ObjectVariableReferenceTranslator implements Function<ObjectVariableReference, ObjectVariableReference> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @Override
    public ObjectVariableReference apply(final ObjectVariableReference objectVariableReference) {
        checkArgument(objectVariableReference.getVariable() instanceof Expression, "Expression variables are supported only");
        return newObjectVariableReferenceBuilder()
                .withVariable((ObjectVariable) translator.apply((Expression) objectVariableReference.getVariable()))
                .build();
    }
}
