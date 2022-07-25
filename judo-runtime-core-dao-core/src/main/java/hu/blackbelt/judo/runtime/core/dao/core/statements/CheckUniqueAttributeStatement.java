package hu.blackbelt.judo.runtime.core.dao.core.statements;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dao.core.values.AttributeValue;
import hu.blackbelt.judo.runtime.core.dao.core.values.InstanceValue;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EClass;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    public static <ID> CheckUniqueAttributeStatement<ID> fromStatement(Statement<ID> statement) {
        CheckUniqueAttributeStatement uniqueStatement = CheckUniqueAttributeStatement.<ID>buildCheckIdentifierStatement()
                .identifier(statement.getInstance().getIdentifier())
                .type(statement.getInstance().getType())
                .build();
        statement.getInstance().getAttributes()
                .forEach(a -> uniqueStatement.getInstance().addAttributeValue(a.getAttribute(), a.getValue()));
        return uniqueStatement;
    }

    public CheckUniqueAttributeStatement mergeAttributes(Statement<ID> statement) {
        List<AttributeValue<Object>> updatedAttributes = new ArrayList<>(this.getInstance().getAttributes());
        List<AttributeValue> attributeToRemove = updatedAttributes.stream()
                .filter(e -> AsmUtils.isIdentifier(e.getAttribute()))
                .filter(a -> statement.getInstance().getAttributes().stream().map(sa -> sa.getAttribute()).collect(Collectors.toSet())
                                .contains(a.getAttribute())).collect(Collectors.toList());
        updatedAttributes.removeAll(attributeToRemove);
        updatedAttributes.addAll(statement.getInstance().getAttributes());

        CheckUniqueAttributeStatement uniqueStatement = CheckUniqueAttributeStatement.<ID>buildCheckIdentifierStatement()
                .identifier(statement.getInstance().getIdentifier())
                .type(statement.getInstance().getType())
                .build();

        updatedAttributes.forEach(a -> this.getInstance().addAttributeValue(a.getAttribute(), a.getValue()));
        return uniqueStatement;
    }


    public String toString() {
        return "CheckUniqueAttributeStatement(" + this.instance.toString() +  ")";
    }
}
