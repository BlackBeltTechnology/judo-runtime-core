package hu.blackbelt.judo.runtime.core.dispatcher;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.InterceptorCallBusinessException;
import org.eclipse.emf.ecore.EOperation;

import java.util.Collection;
import java.util.Collections;

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
     * Get operations which this interceptor should trigger for.
     * If it's empty, all operation calls will be intercepted.
     *
     * @param asmModel - The ASM model which this interceptor belongs to

     * @return
     */
    default Collection<EOperation> getOperations(AsmModel asmModel) {
        return Collections.emptyList();
    }

    /**
     * If true, the interceptor will be called outside of transaction context and no context
     * variables will be available, it performed on a different thread.
     *
     * @return
     */
    default boolean async() { return false; };

    /**
     * The interceptor will terminate the execution of the call when an error occurred.
     * This parameter is ignored on async calls.
     *
     * @return
     */
    default boolean terminateOnException() { return true;};


    /**
     * When returns true it will ignore the system defined DAO call.
     *
     * @return
     */
    default boolean ignoreDecoratedCall() { return false;};

    /**
     * Called before the original decorated function call.
     * @param operation - Which operation is performed.
     * @param parameterPayload - The operation call input parameter
     *
     * @return The payload which will be used to perform the call. When ignoreDecoratedCall is true
     * it has to return the decorated call return type's payload.
     * @throws InterceptorCallBusinessException serializable behaviour exception.
     */
    default Object preCall(EOperation operation, Object parameterPayload) throws InterceptorCallBusinessException {
        return parameterPayload;
    };

    /**
     * Called after the original decorated function call.
     * @param operation - Which operation is performed
     * @param parameterPayload - The operation call input parameter
     * @param returnPayload - The operation call output parameter. It can be list or payload.
     * @return The {@link Payload} or {@link Collection} of {@link Payload} which will be returned of the call.
     * @throws InterceptorCallBusinessException serializable behaviour exception.
     */
    default Object postCall(EOperation operation, Object parameterPayload, Object returnPayload) throws InterceptorCallBusinessException {
        return returnPayload;
    }

}
