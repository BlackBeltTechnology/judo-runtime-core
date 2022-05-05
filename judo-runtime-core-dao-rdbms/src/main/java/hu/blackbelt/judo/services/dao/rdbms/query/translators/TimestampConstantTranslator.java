package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.constant.TimestampConstant;
import lombok.Builder;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.function.Function;

@Builder
public class TimestampConstantTranslator implements Function<TimestampConstant, TimestampConstant> {

    @Override
    public TimestampConstant apply(final TimestampConstant constant) {
        return EcoreUtil.copy(constant);
    }
}
