package hu.blackbelt.judo.services.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.expression.constant.CustomData;
import hu.blackbelt.judo.meta.expression.constant.Literal;
import lombok.Builder;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.function.Function;

@Builder
public class CustomDataTranslator implements Function<CustomData, CustomData> {

    @Override
    public CustomData apply(final CustomData constant) {
        return EcoreUtil.copy(constant);
    }
}
