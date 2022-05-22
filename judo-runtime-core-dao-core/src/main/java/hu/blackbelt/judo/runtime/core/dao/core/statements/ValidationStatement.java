package hu.blackbelt.judo.runtime.core.dao.core.statements;

import hu.blackbelt.judo.runtime.core.dao.core.values.InstanceValue;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EClass;

@Getter
public class ValidationStatement<ID> extends Statement<ID> {

    @Builder(builderMethodName = "buildValidationStatement")
    public ValidationStatement(EClass type, ID identifier) {
        super(InstanceValue
                .<ID>buildInstanceValue()
                    .type(type)
                    .identifier(identifier)
                    .build());
    }

    public String toString() {
        return "ValidationStatement(" + this.instance.toString() +  ")";
    }
}
