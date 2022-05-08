package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.ActorVariableProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.CurrentDateProvider;

import java.time.LocalDate;
import java.util.function.Function;
import java.util.function.Supplier;

public class CurrentDateProviderProvider implements Provider<Supplier<LocalDate>> {

    Context context;

    @Inject
    public CurrentDateProviderProvider(Context context) {
        this.context = context;
    }

    @Override
    public Supplier<LocalDate> get() {
        return new CurrentDateProvider();
    }
}
