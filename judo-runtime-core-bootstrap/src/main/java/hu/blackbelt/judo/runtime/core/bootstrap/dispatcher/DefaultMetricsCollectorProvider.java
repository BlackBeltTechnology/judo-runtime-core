package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultMetricsCollector;

import javax.annotation.Nullable;
import java.util.function.Consumer;

public class DefaultMetricsCollectorProvider implements Provider<MetricsCollector> {

    public static final String METRICS_COLLECTOR_CONSUMER = "metricsCollectorConsumer";
    public static final String METRICS_COLLECTOR_ENABLED = "metricsCollectorEnabled";
    public static final String METRICS_COLLECTOR_VERBOSE = "metricsCollectorVerbose";

    @Inject
    Context context;

    @SuppressWarnings("rawtypes")
	@Inject(optional = true)
    @Named(METRICS_COLLECTOR_CONSUMER)
    @Nullable
    Consumer metricsConsumer = (m) -> {};

    @Inject(optional = true)
    @Named(METRICS_COLLECTOR_ENABLED)
    @Nullable
    Boolean enabled = false;

    @Inject(optional = true)
    @Named(METRICS_COLLECTOR_VERBOSE)
    @Nullable
    Boolean verbose = false;

    @SuppressWarnings("unchecked")
	@Override
    public MetricsCollector get() {
        return DefaultMetricsCollector.builder()
                .context(context)
                .metricsConsumer(metricsConsumer)
                .enabled(enabled)
                .verbose(verbose)
                .build();
    }
}
