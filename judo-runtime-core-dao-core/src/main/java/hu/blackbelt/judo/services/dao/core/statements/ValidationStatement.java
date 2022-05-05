package hu.blackbelt.judo.services.dao.core.statements;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.services.dao.core.values.InstanceValue;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.eclipse.emf.ecore.EClass;

@Getter
@ToString
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
