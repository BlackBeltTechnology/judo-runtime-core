package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.constant.DateConstant;
import lombok.Builder;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.function.Function;

@Builder
public class DateConstantTranslator implements Function<DateConstant, DateConstant> {

    @Override
    public DateConstant apply(final DateConstant constant) {
        return EcoreUtil.copy(constant);
    }
}
