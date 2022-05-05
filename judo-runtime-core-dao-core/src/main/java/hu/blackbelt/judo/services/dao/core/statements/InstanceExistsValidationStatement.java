package hu.blackbelt.judo.services.dao.core.statements;

import hu.blackbelt.judo.dao.api.Payload;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.eclipse.emf.ecore.EClass;

@Getter
@ToString
public class InstanceExistsValidationStatement<ID> extends ValidationStatement<ID> {

    @Builder(builderMethodName = "buildInstanceExistsValidationStatement")
    public InstanceExistsValidationStatement(EClass type, ID identifier) {
        super(type, identifier);
    }

    public String toString() {
        return "InstanceExistsValidationStatement(" + this.instance.toString() +  ")";
    }
}
