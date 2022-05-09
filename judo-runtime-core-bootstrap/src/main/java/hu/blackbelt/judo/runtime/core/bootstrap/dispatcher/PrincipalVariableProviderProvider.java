package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.ActorVariableProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.PrincipalVariableProvider;

import java.util.function.Function;

public class PrincipalVariableProviderProvider implements Provider<Function<String, Object>> {

    @Inject
    Context context;

    @Inject
    AsmModel asmModel;

    @Inject
    Dispatcher dispatcher;

    @Override
    public Function<String, Object> get() {
        return PrincipalVariableProvider.builder()
                .context(context)
                .asmModel(asmModel)
                .dispatcher(dispatcher)
                .build();
    }
}
