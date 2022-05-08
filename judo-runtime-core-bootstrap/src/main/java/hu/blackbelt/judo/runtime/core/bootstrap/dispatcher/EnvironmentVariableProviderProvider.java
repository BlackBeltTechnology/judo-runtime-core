package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.AccessTokenVariableProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.EnvironmentVariableProvider;

import java.util.function.Function;

public class EnvironmentVariableProviderProvider implements Provider<Function<String, Object>> {

    @Override
    public Function<String, Object> get() {
        return new EnvironmentVariableProvider();
    }
}
