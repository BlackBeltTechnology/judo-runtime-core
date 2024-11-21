package hu.blackbelt.judo.runtime.core.accessmanager.api;

import org.eclipse.emf.ecore.EOperation;

import java.util.Map;

/**
 * This interface defines an interceptor for authentication / authorization call.
 */
public interface AuthenticationInterceptor {
    String getName();

    default boolean isSuitableForOperation(EOperation operation, final String claim,
                                   final String realm, final String client, Map<String, Object> attributes) {
        return true;
    }

    /**
     * It is called after the extraction of principal but before the load of the mapped principal load.
     * CAUTION! From the implementation of this method expressions which uses actor cannot be used.
     * (for example getVariables for actor).
     *
     * @param operationFullyQualifiedName The called operation's fully qualified name
     * @param exchange The request exchange
     * @param claim The user's name
     * @param realm Realm authenticated
     * @param client Authenticated client
     * @param attributes Token attributes
     */
    default void authenticate(final String operationFullyQualifiedName,
                 final Map<String, Object> exchange,
                 final String claim,
                 final String realm,
                 final String client,
                 final Map<String, Object> attributes
    ) {};


    /**
     * It is called after successfully authorization, but before operation call,when the given operation have to be checked for
     * authorization.
     *
     * @param operation The called operation's fully qualified name
     * @param signedIdentifier The mapped operation's owner signed identifier
     * @param exchange The request exchange
     * @param claim The user's name
     * @param realm Realm authenticated
     * @param client Authenticated client
     * @param attributes Token attributes
     */
    default void success(final EOperation operation,
                 final SignedIdentifier signedIdentifier,
                 final Map<String, Object> exchange,
                 final String claim,
                 final String realm,
                 final String client,
                 final Map<String, Object> attributes
    ) {};
}
