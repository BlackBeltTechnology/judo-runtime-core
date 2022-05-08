package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultMetricsCollector;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class DefaultMetricsCollectorProvider implements Provider<MetricsCollector> {

    public static final String METRICS_COLLECTOR_CONSUMER = "metricsCollectorConsumer";
    public static final String METRICS_COLLECTOR_ENABLED = "metricsCollectorEnabled";
    public static final String METRICS_COLLECTOR_VERBOSE = "metricsCollectorVerbose";
    Context context;
    Consumer metricsConsumer;
    Boolean enabled;
    Boolean verbose;

    @Inject
    public DefaultMetricsCollectorProvider(Context context,
                                           @Named(METRICS_COLLECTOR_CONSUMER) Consumer metricsConsumer,
                                           @Named(METRICS_COLLECTOR_ENABLED) Boolean enabled,
                                           @Named(METRICS_COLLECTOR_VERBOSE) Boolean verbose) {
        this.context = context;
        this.metricsConsumer = metricsConsumer;
        this.enabled = enabled;
        this.verbose = verbose;
    }

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
