package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.constant.BooleanConstant;
import hu.blackbelt.judo.meta.expression.constant.DecimalConstant;
import lombok.Builder;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.function.Function;

@Builder
public class BooleanConstantTranslator implements Function<BooleanConstant, BooleanConstant> {

    @Override
    public BooleanConstant apply(final BooleanConstant constant) {
        return EcoreUtil.copy(constant);
    }
}
