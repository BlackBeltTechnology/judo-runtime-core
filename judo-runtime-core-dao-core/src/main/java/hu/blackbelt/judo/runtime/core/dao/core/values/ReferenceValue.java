package hu.blackbelt.judo.runtime.core.dao.core.values;

import com.google.common.collect.ImmutableList;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.Collection;

@Builder(builderMethodName = "referenceValueBuilder")
@Getter
public class ReferenceValue<ID> {

    @NonNull
    EClass type;

    @NonNull
    ID identifier;

    @NonNull
    EReference reference;

    @Builder.Default
    Collection<ID> oppositeIdentifiers = ImmutableList.of();


    public String toString() {
        return "(type=" + AsmUtils.getClassifierFQName(this.getType()) +
                ", identifier=" + this.getIdentifier() +
                ", reference=" + AsmUtils.getReferenceFQName(this.getReference()) +
                ", oppositeIdentifiers=" + this.getOppositeIdentifiers() + ")";
    }
}
