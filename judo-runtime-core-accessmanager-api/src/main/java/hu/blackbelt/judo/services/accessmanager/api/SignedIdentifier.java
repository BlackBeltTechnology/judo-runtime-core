package hu.blackbelt.judo.services.accessmanager.api;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.ETypedElement;

@Getter
@Builder
public class SignedIdentifier {

    @NonNull
    private final String identifier;

    private final ETypedElement producedBy;

    private final String entityType;

    private final Integer version;

    private final Boolean immutable;
}
