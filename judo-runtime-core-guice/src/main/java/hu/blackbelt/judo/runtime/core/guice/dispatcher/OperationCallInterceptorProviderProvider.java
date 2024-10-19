package hu.blackbelt.judo.runtime.core.guice.dispatcher;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;

import java.util.ArrayList;
import java.util.List;

public class OperationCallInterceptorProviderProvider implements Provider<OperationCallInterceptorProvider> {
    @Override
    public OperationCallInterceptorProvider get() {
        final List<OperationCallInterceptor> interceptors = new ArrayList<>();

        return new OperationCallInterceptorProvider() {
            @Override
            public List<OperationCallInterceptor> getCallOperationInterceptors() {
                return interceptors;
            }
        };
    }
}
