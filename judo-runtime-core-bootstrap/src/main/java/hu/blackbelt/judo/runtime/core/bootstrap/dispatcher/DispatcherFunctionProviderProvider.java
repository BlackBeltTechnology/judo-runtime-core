package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.dispatcher.DispatcherFunctionProvider;
import org.eclipse.emf.common.util.BasicEMap;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EOperation;

import java.util.function.Function;

public class DispatcherFunctionProviderProvider implements Provider<DispatcherFunctionProvider> {
    @Override
    public DispatcherFunctionProvider get() {
        final EMap<EOperation, Function<Payload, Payload>> scripts = new BasicEMap<>();
        DispatcherFunctionProvider dispatcherFunctionProvider = new DispatcherFunctionProvider() {
            @Override
            public EMap<EOperation, Function<Payload, Payload>> getSdkFunctions() {
                return ECollections.emptyEMap();
            }

            @Override
            public EMap<EOperation, Function<Payload, Payload>> getScriptFunctions() {
                return scripts;
            }
        };
        return dispatcherFunctionProvider;
    }
}
