package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.constant.TimeConstant;
import lombok.Builder;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.function.Function;

@Builder
public class TimeConstantTranslator implements Function<TimeConstant, TimeConstant> {

    @Override
    public TimeConstant apply(final TimeConstant constant) {
        return EcoreUtil.copy(constant);
    }
}
