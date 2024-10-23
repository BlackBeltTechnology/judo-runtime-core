package hu.blackbelt.judo.runtime.core.dispatcher;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.InterceptorCallBusinessException;
import org.eclipse.emf.ecore.EOperation;

import java.util.concurrent.CompletableFuture;

public class CallInterceptorUtil<P, R> {

    private final Class<P> parameterType;
    private final Class<R> returnType;

    private final AsmModel asmModel;
    private final EOperation operation;
    private final OperationCallInterceptorProvider interceptorProvider;

    @SuppressWarnings({"unchecked"})
    public CallInterceptorUtil(Class<? super P> parameterType, Class<? super R> returnType, AsmModel asmModel,
                               EOperation operation,
                               OperationCallInterceptorProvider interceptorProvider) {
        this.parameterType = (Class<P>) parameterType;
        this.returnType = (Class<R>) returnType;
        this.asmModel = asmModel;
        this.operation = operation;
        this.interceptorProvider = interceptorProvider;

    }

    public boolean shouldCallOriginal() {
        boolean callDecorated = true;
        for (OperationCallInterceptor interceptor : interceptorProvider.getInterceptorsForOperation(asmModel, operation)) {
            callDecorated = callDecorated & !interceptor.ignoreDecoratedCall();
        }
        return callDecorated;
    }


    public P preCallInterceptors(P inputParameter) throws InterceptorCallBusinessException {
        P ret = inputParameter;
        boolean callDecorated = true;
        for (OperationCallInterceptor interceptor : interceptorProvider.getInterceptorsForOperation(asmModel, operation)) {
            callDecorated = callDecorated && !interceptor.ignoreDecoratedCall();
            if (!interceptor.async()) {
                Object o = interceptor.preCall(operation, inputParameter);
                if (o == null || parameterType.isAssignableFrom(o.getClass())) {
                    ret = (P) o;
                } else {
                    throw new IllegalArgumentException("The interceptor call return type: " + o.getClass().getName()
                            + " does not match with expected type: " + parameterType.getName());
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



    public R postCallInterceptors(P inputParameter,
                                               R returnPayload) throws InterceptorCallBusinessException {
        R ret = returnPayload;
        boolean callDecorated = true;
        for (OperationCallInterceptor interceptor : interceptorProvider.getInterceptorsForOperation(asmModel, operation)) {
            callDecorated = callDecorated && !interceptor.ignoreDecoratedCall();
            if (!interceptor.async()) {
                Object o  = interceptor.postCall(operation, inputParameter, returnPayload);
                if (o == null || returnType.isAssignableFrom(o.getClass())) {
                    ret = (R) o;
                } else {
                    throw new IllegalArgumentException("The interceptor call return type: " + o.getClass().getName()
                            + " does not match with expected type: " + returnType.getName());
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
