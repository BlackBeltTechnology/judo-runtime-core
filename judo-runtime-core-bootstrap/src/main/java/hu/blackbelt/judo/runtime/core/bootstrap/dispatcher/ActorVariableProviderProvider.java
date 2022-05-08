package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.ActorVariableProvider;

import java.util.function.Function;

public class ActorVariableProviderProvider implements Provider<Function<String, Object>> {

    Context context;

    @Inject
    public ActorVariableProviderProvider(Context context) {
        this.context = context;
    }

    @Override
    public Function<String, Object> get() {
        return new ActorVariableProvider(context);
    }
}
