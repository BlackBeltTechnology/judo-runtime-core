package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.CurrentDateProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.CurrentTimeProvider;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.function.Supplier;

public class CurrentTimeProviderProvider implements Provider<Supplier<LocalTime>> {

    Context context;

    @Inject
    public CurrentTimeProviderProvider(Context context) {
        this.context = context;
    }

    @Override
    public Supplier<LocalTime> get() {
        return new CurrentTimeProvider();
    }
}
