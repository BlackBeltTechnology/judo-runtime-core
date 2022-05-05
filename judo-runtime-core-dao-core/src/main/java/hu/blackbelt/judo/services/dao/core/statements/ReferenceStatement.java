package hu.blackbelt.judo.services.dao.core.statements;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.services.dao.core.values.InstanceValue;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

@Getter
public abstract class ReferenceStatement<ID> extends Statement<ID> {

    @NonNull
    ID identifier;

    @NonNull
    EReference reference;

    public ReferenceStatement(EClass type, EReference reference, ID identifier, ID referenceIdentifier) {
        super(InstanceValue
                        .<ID>buildInstanceValue()
                            .type(type)
                            .identifier(referenceIdentifier)
                            .build());
        this.identifier = identifier;
        this.reference = reference;
    }


    public String toString() {
        return "ReferenceStatement(" +
                "identifier=" + this.getIdentifier() +
                ", reference=" + AsmUtils.getReferenceFQName(this.getReference()) +
                ", instance=" + this.instance.toString() + ")";
    }
}
