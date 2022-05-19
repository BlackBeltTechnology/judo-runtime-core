package hu.blackbelt.judo.runtime.core.dao.core.statements;

import hu.blackbelt.judo.runtime.core.dao.core.values.InstanceValue;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EClass;

import java.time.OffsetDateTime;

@Getter
public class UpdateStatement<ID> extends Statement<ID> {

    private final Integer version;
    private final ID userId;
    private final String userName;
    private final OffsetDateTime timestamp;

    @Builder(builderMethodName = "buildUpdateStatement")
    public UpdateStatement(
            EClass type,
            ID identifier,
            Integer version,
            ID userId,
            String username,
            OffsetDateTime timestamp
    ) {

        super(InstanceValue
                        .<ID>buildInstanceValue()
                            .type(type)
                            .identifier(identifier)
                            .build());
        this.version = version;
        this.userId = userId;
        this.userName = username;
        this.timestamp = timestamp;
    }

    public String toString() {
        return "UpdateStatement(" + this.instance.toString() +  ")";
    }
}
