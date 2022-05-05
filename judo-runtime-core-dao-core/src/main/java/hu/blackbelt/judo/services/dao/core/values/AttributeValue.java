package hu.blackbelt.judo.services.dao.core.values;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.EAttribute;

@Builder(builderMethodName = "attributeValueBuilder")
@Getter
public class AttributeValue<O> {

    @NonNull
    EAttribute attribute;
    O value;

    public String toString() {
        return "AttributeValue(" + this.getAttribute().getName() + "=" + this.getValue() + ")";
    }
}
