package hu.blackbelt.judo.runtime.core.accessmanager.api;

import org.eclipse.emf.ecore.EOperation;

import java.util.Map;

public interface AuthenticationInterceptor {
    String getName();

    boolean isSuitableForOperation(EOperation operation, final String claim,
                                   final String realm, final String client, Map<String, Object> attributes);

    void success(final EOperation operation,
                 final SignedIdentifier signedIdentifier,
                 final Map<String, Object> exchange,
                 final String claim,
                 final String realm,
                 final String client,
                 final Map<String, Object> attributes
    );
}
