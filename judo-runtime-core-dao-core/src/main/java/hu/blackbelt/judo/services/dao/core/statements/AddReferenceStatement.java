package hu.blackbelt.judo.services.dao.core.statements;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.Collection;

@Getter
public class AddReferenceStatement<ID> extends ReferenceStatement<ID> {

    Collection<ID> alreadyReferencingInstances;

    @Builder(builderMethodName = "buildAddReferenceStatement")
    public AddReferenceStatement(EClass type, EReference reference, ID identifier, ID referenceIdentifier, Collection<ID> alreadyReferencingInstances) {
        super(type, reference, identifier, referenceIdentifier);
        this.alreadyReferencingInstances = alreadyReferencingInstances;
    }

    public String toString() {
        return "AddReferenceStatement(" +
                "identifier=" + this.getIdentifier() +
                ", alreadyReferencingInstances=" + alreadyReferencingInstances +
                ", reference=" + AsmUtils.getReferenceFQName(this.getReference()) +
                ", instance=" + this.instance.toString() + ")";
    }
}
