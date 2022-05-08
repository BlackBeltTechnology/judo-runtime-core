package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.CurrentTimeProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.CurrentTimestampProvider;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.function.Supplier;

public class CurrentTimestampProviderProvider implements Provider<Supplier<OffsetDateTime>> {

    Context context;

    @Inject
    public CurrentTimestampProviderProvider(Context context) {
        this.context = context;
    }

    @Override
    public Supplier<OffsetDateTime> get() {
        return new CurrentTimestampProvider();
    }
}
