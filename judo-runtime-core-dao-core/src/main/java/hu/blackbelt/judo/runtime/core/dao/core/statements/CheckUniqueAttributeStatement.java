package hu.blackbelt.judo.runtime.core.dao.core.statements;

import hu.blackbelt.judo.runtime.core.dao.core.values.InstanceValue;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EClass;

@Getter
public class CheckUniqueAttributeStatement<ID> extends Statement<ID> {

    @Builder(builderMethodName = "buildCheckIdentifierStatement")
    public CheckUniqueAttributeStatement(
            EClass type,
            ID identifier
    ) {

        super(InstanceValue
                        .<ID>buildInstanceValue()
                            .type(type)
                            .identifier(identifier)
                            .build());
    }

    public String toString() {
        return "CheckUniqueAttributeStatement(" + this.instance.toString() +  ")";
    }
}
