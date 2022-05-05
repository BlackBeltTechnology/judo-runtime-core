package hu.blackbelt.judo.services.accessmanager.api;

import org.eclipse.emf.ecore.EOperation;

import java.util.Map;

/**
 * Access manager is used to check if a given operation call is allowed.
 */
public interface AccessManager {

    /**
     * Authorize an operation call.
     *
     * @param operation operation to call
     * @param signedIdentifier signed identifier of bound operation
     * @param exchange exchange
     */
    void authorizeOperation(EOperation operation, SignedIdentifier signedIdentifier, Map<String, Object> exchange);
}
