package hu.blackbelt.judo.runtime.core.guice.dispatcher;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;

public class OperationCallInterceptorProviderProvider implements Provider<OperationCallInterceptorProvider> {
    @Override
    public OperationCallInterceptorProvider get() {
        final EList<OperationCallInterceptor> interceptors = new BasicEList<>();

        return new OperationCallInterceptorProvider() {
            @Override
            public EList<OperationCallInterceptor> getCallOperationInterceptors() {
                return interceptors;
            }
        };
    }
}
