package hu.blackbelt.judo.runtime.core.dao.core.statements;

import hu.blackbelt.judo.runtime.core.dao.core.values.InstanceValue;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.time.OffsetDateTime;
import java.util.Optional;


@Getter
public class InsertStatement<ID> extends Statement<ID> {

    private final EReference container;
    private final Object clientReferenceIdentifier;
    private final Integer version;
    private final ID userId;
    private final String userName;
    private final OffsetDateTime timestamp;

    @Builder(builderMethodName = "buildInsertStatement")
    public InsertStatement(
            EClass type,
            ID identifier,
            Object clientReferenceIdentifier,
            EReference container,
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

        this.container = container;
        this.clientReferenceIdentifier = clientReferenceIdentifier;
        this.version = version;
        this.userId = userId;
        this.userName = username;
        this.timestamp = timestamp;
    }

    public Optional<EReference> getContainer() {
        return Optional.ofNullable(container);
    }

    public String toString() {
        return "InsertStatement(" + this.instance.toString() +  ")";
    }
}
