package hu.blackbelt.judo.services.core;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MetricsCancelToken implements AutoCloseable {

    private final String keyOfMeasurement;
    private final MetricsCollector metricsCollector;

    @Override
    public void close() {
        metricsCollector.stop(keyOfMeasurement);
    }

}