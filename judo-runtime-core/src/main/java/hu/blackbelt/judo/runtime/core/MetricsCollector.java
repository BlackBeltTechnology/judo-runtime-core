package hu.blackbelt.judo.runtime.core;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector.
 */
public interface MetricsCollector {

    String FRAMEWORK_METRICS = "FRAMEWORK_METRICS";

    /**
     * Starts a new (nested) measurement with the given key.
     * @param key the key of the measurement to be started
     * @return MetricsCancelToken that can be used to auto stop measurement in a try block.
     */
    MetricsCancelToken start(String key);

    /**
     * Stops the measurement with the given key.
     * @param key the key of the measurement to be stopped
     * @throws IllegalStateException if the key of the measurement does not match that of the last started one
     */
    void stop(String key) throws IllegalStateException;

    /**
     * Returns the so far collected metrics.
     */
    Map<String, AtomicLong> getMetrics();
}
