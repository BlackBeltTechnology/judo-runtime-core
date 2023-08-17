package hu.blackbelt.judo.runtime.core.dispatcher;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.ecore.EOperation;

import java.util.Collection;

/**
 * Interface to register decorator functions over operation calls. It supports
 * - Behaviour calls (aka CRUD operations like create, validate etc.)
 * - Script operations (scripts defined inside model)
 * - SDK implementations (java implemented methods).
 *
 */
public interface OperationCallInterceptor {

    /**
     * Get interceptor name
     * @return
     */
    String getName();

    /**
     * Get operations which this interceptor suitable for. It its empty
     * all operation call will be intercepted.
     *
     * @param asmModel - Model interceptor belongs

     * @return
     */
    default Collection<EOperation> getOperations(AsmModel asmModel) {
        return ECollections.emptyEList();
    }

    /**
     * The interceptor called as async operation. It will be called
     * outside of transaction context and no context variables will
     * be available.
     *
     * @return
     */
    default boolean async() { return false; };

    /**
     * The interceptor will terminate the execution of call when an error occurred.
     * This parameter is ignored on async calls.
     *
     * @return
     */
    default boolean terminateOnException() { return true;};


    /**
     * When returns true it will ignore the original call and preCall return
     * payload will be used to return type.
     * @return
     */
    default boolean ignoreDecoratedCall() { return false;};

    /**
     * Called before the original decorated function call.
     * @param operation - Which operation is performed.
     * @param parameterPayload - The operation call input parameter
     *
     * @return The payload which will be used to perform the call. When ignoreDecoratedCall is true
     * it have to return the decorated call return type's payload.
     */
    Object preCall(EOperation operation, Payload parameterPayload);

    /**
     * Called after the original decorated function call.
     * @param operation - Which operation is performed
     * @param parameterPayload - The operation call input parameter
     * @param returnPayload - The operation call output parameter
     * @param returnPayload - The return payload
     * @return The payload which will be returned of the call.
     */
    Object postCall(EOperation operation, Payload parameterPayload, Payload returnPayload);

}
