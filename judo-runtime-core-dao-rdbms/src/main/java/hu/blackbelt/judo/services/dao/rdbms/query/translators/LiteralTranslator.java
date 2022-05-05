package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.constant.Literal;
import lombok.Builder;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.function.Function;

@Builder
public class LiteralTranslator implements Function<Literal, Literal> {

    @Override
    public Literal apply(final Literal constant) {
        return EcoreUtil.copy(constant);
    }
}
