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

    @Builder.Default
    EReference container = null;

    @Builder.Default
    Object clientReferenceIdentifier = null;

    @Builder.Default
    Integer version = 1;

    @Builder.Default
    ID userId = null;

    @Builder.Default
    String userName = null;

    @Builder.Default
    OffsetDateTime timestamp = null;

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
