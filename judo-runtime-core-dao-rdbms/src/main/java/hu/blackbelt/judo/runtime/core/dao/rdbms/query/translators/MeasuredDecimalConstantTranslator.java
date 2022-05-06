package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.constant.MeasuredDecimal;
import lombok.Builder;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.function.Function;

@Builder
public class MeasuredDecimalConstantTranslator implements Function<MeasuredDecimal, MeasuredDecimal> {

    @Override
    public MeasuredDecimal apply(final MeasuredDecimal constant) {
        return EcoreUtil.copy(constant);
    }
}
