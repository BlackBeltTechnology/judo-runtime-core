package hu.blackbelt.judo.services.dao.core.values;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

/**
 * Represents a statement instance. It can contain attributes and references. All statement have minimum one instance
 * to work with.
 * @param <ID>
 */
@Builder(builderMethodName = "buildInstanceValue")
@Getter
@EqualsAndHashCode
public class InstanceValue<ID> {
    @NonNull
    EClass type;

    @NonNull
    ID identifier;

    @Builder.Default
    List<AttributeValue> attributes = newArrayList();

    public void addAttributeValue(EAttribute attribute, Object value) {
        attributes.add(AttributeValue
                .attributeValueBuilder()
                    .attribute(attribute)
                    .value(value)
                    .build());
    }

    public String toString() {
        return "InstanceValue(type=" + AsmUtils.getClassifierFQName(this.getType()) +
                ", identifier=" + this.getIdentifier() +
                ", attributes=" + this.getAttributes() +
                ")";
    }
}
