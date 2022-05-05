package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.constant.StringConstant;
import lombok.Builder;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.function.Function;

@Builder
public class StringConstantTranslator implements Function<StringConstant, StringConstant> {

    @Override
    public StringConstant apply(final StringConstant constant) {
        return EcoreUtil.copy(constant);
    }
}
