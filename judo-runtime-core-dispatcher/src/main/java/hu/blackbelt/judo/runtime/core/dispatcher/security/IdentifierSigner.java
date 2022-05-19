package hu.blackbelt.judo.runtime.core.dispatcher.security;

import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.ETypedElement;

import java.util.Map;
import java.util.Optional;

public interface IdentifierSigner {

    String SIGNED_IDENTIFIER_KEY = "__signedIdentifier";

    void signIdentifiers(ETypedElement typedElement, Map<String, Object> payload, boolean immutable);

    Optional<SignedIdentifier> extractSignedIdentifier(EClass transferObjectType, Map<String, Object> payload);
}
