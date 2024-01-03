package hu.blackbelt.judo.runtime.core.dagger2.dispatcher;

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;

@Module
public class OperationCallInterceptorProviderModule {

    @JudoApplicationScope
    @Provides
    public OperationCallInterceptorProvider providesOperationCallInterceptorProvider() {
        final EList<OperationCallInterceptor> interceptors = new BasicEList<>();

        return new OperationCallInterceptorProvider() {
            @Override
            public EList<OperationCallInterceptor> getCallOperationInterceptors() {
                return interceptors;
            }
        };
    }
}
