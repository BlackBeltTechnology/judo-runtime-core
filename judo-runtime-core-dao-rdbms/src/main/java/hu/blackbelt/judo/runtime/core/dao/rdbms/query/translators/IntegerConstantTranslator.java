package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.constant.IntegerConstant;
import lombok.Builder;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.function.Function;

@Builder
public class IntegerConstantTranslator implements Function<IntegerConstant, IntegerConstant> {

    @Override
    public IntegerConstant apply(final IntegerConstant constant) {
        return EcoreUtil.copy(constant);
    }
}
