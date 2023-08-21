package hu.blackbelt.judo.runtime.core.dispatcher;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import org.eclipse.emf.ecore.EOperation;

import java.util.concurrent.CompletableFuture;

public class CallInterceptorUtil {

    public static boolean isOriginalCalled(AsmModel asmModel,
                                             EOperation operation,
                                             OperationCallInterceptorProvider interceptorProvider) {
        boolean callDecorated = true;
        for (OperationCallInterceptor interceptor : interceptorProvider.getInterceptorsForOperation(asmModel, operation)) {
            callDecorated = callDecorated & !interceptor.ignoreDecoratedCall();
        }
        return callDecorated;
    }


    public static <P> P preCallInterceptors(Class<P> type,
                                                AsmModel asmModel,
                                              EOperation operation,
                                              OperationCallInterceptorProvider interceptorProvider,
                                              P inputParameter) {
        P ret = inputParameter;
        boolean callDecorated = true;
        for (OperationCallInterceptor interceptor : interceptorProvider.getInterceptorsForOperation(asmModel, operation)) {
            callDecorated = callDecorated & !interceptor.ignoreDecoratedCall();
            if (!interceptor.async()) {
                Object o = interceptor.preCall(operation, inputParameter);
                if (o == null || type.isAssignableFrom(o.getClass())) {
                    ret = (P) o;
                } else {
                    throw new IllegalArgumentException("The interceptor call return type: " + o.getClass().getName() + " does not math with expected type: " + type.getName());
                }
            } else {
                Object finalInputParameter = inputParameter;
                CompletableFuture.runAsync(() -> {
                    interceptor.preCall(operation, finalInputParameter);
                });
            }
        }
        return ret;
    }


    public static <R, P> R postCallInterceptors(Class<P> parameterType,
                                                Class<R> returnType,
                                                AsmModel asmModel,
                                               EOperation operation,
                                               OperationCallInterceptorProvider interceptorProvider,
                                               P inputParameter,
                                               R returnPayload) {
        R ret = returnPayload;
        boolean callDecorated = true;
        for (OperationCallInterceptor interceptor : interceptorProvider.getInterceptorsForOperation(asmModel, operation)) {
            callDecorated = callDecorated & !interceptor.ignoreDecoratedCall();
            if (!interceptor.async()) {
                Object o  = interceptor.postCall(operation, inputParameter, returnPayload);
                if (o == null || returnType.isAssignableFrom(o.getClass())) {
                    ret = (R) o;
                } else {
                    throw new IllegalArgumentException("The interceptor call return type: " + o.getClass().getName() + " does not math with expected type: " + returnType.getName());
                }
            } else {
                Object finalInputParameter = inputParameter;
                CompletableFuture.runAsync(() -> {
                    interceptor.preCall(operation, finalInputParameter);
                });
            }
        }
        return ret;
    }

}
