package hu.blackbelt.judo.runtime.core.dao.core.statements;

import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EClass;

@Getter
public class InstanceExistsValidationStatement<ID> extends ValidationStatement<ID> {

    @Builder(builderMethodName = "buildInstanceExistsValidationStatement")
    public InstanceExistsValidationStatement(EClass type, ID identifier) {
        super(type, identifier);
    }

    public String toString() {
        return "InstanceExistsValidationStatement(" + this.instance.toString() +  ")";
    }
}
