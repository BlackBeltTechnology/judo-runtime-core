package hu.blackbelt.judo.services.dao.core.statements;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.*;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

@Getter
@ToString
public class RemoveReferenceStatement<ID> extends ReferenceStatement<ID> {

    @Builder(builderMethodName = "buildRemoveReferenceStatement")
    public RemoveReferenceStatement(EClass type, EReference reference, ID identifier, ID referenceIdentifier) {
        super(type, reference, identifier, referenceIdentifier);
    }

    public String toString() {
        return "RemoveReferenceStatement(" +
                "identifier=" + this.getIdentifier() +
                ", reference=" + AsmUtils.getReferenceFQName(this.getReference()) +
                ", instance=" + this.instance.toString() + ")";
    }
}
