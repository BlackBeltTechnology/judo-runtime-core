package hu.blackbelt.judo.runtime.core.dispatcher;

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.MetricsCancelToken;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
public class DefaultMetricsCollector implements MetricsCollector {


    private static final Logger metricsLogger = LoggerFactory.getLogger("metrics");
    private static final String FRAMEWORK_METRICS_STACK = FRAMEWORK_METRICS + "_stack";

//    @Setter
//    public EventAdmin eventAdmin;

    @Setter
    Context context;

    @Setter
    private boolean enabled;

    @Setter
    private boolean verbose;

    private String modelName;

    /*
    @Activate
    void activate(final Config config) {
        enabled = config.enabled();
        verbose = config.verbose();
        modelName = config.judo_model_name();
    }
    */

    @Override
    public MetricsCancelToken start(final String key) {
        if (enabled) {
            context.putIfAbsent(FRAMEWORK_METRICS, new TreeMap<>());
            context.putIfAbsent(FRAMEWORK_METRICS_STACK, new Stack<>());
            Stack<StackEntry> stack = context.getAs(Stack.class, FRAMEWORK_METRICS_STACK);
            stack.push(new StackEntry(key));
            log.debug("Started collector with key: {}", key);
        }
        return new MetricsCancelToken(key, this);
    }

    @Override
    public void stop(String key) {
        if (enabled) {
            final Stack stack = context.getAs(Stack.class, FRAMEWORK_METRICS_STACK);
            if (!stack.empty()) {
                final StackEntry entry = (StackEntry)stack.pop();
                if (!entry.key.equals(key)) {
                    throw new IllegalStateException("The measurement to be stopped is not the same that was last started.");
                }
                final long delta = System.nanoTime() - entry.startTimestamp;
                incrementCounter(entry.key, delta - entry.nested.longValue());
                log.debug("Stopped collector with key: {}, delta = {}", entry.key, delta);
                if (stack.isEmpty()) {
                    submit();
                } else {
                    ((StackEntry)stack.peek()).nested.addAndGet(delta);
                    log.trace("Remaining collectors: {}", stack);
                }
            } else {
                log.error("No metrics collecting in progress");
            }
        }
    }

    @Override
    public Map<String, AtomicLong> getMetrics() {
        if (enabled) {
            final Map<String, AtomicLong> metrics = context.getAs(Map.class, FRAMEWORK_METRICS);
            return metrics != null ? Collections.unmodifiableMap(metrics.entrySet().stream()
                    .filter(e -> verbose || !e.getKey().startsWith("_"))
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()))) : Collections.emptyMap();
        } else {
            return Collections.emptyMap();
        }
    }

    private void incrementCounter(final String key, final long delta) {
        final Map<String, AtomicLong> metrics = context.getAs(Map.class, FRAMEWORK_METRICS);
        if (metrics != null) {
            log.trace("Increment collector: {}, delta = {}", key, delta);
            final AtomicLong counter;
            if (metrics.containsKey(key)) {
                counter = metrics.get(key);
            } else {
                counter = new AtomicLong(0);
                metrics.put(key, counter);
            }
            counter.addAndGet(delta);
        }
    }

    public void submit() {
        if (enabled) {
            final Map<String, AtomicLong> metrics = Optional.ofNullable(context.getAs(Map.class, FRAMEWORK_METRICS)).orElse(Collections.emptyMap());
            /*
            if (eventAdmin != null) {
                metrics.entrySet().stream()
                        .filter(e -> verbose || !e.getKey().startsWith("_"))
                        .forEach(entry -> {
                            final Map<String, Object> data = new HashMap<>();
                            data.put("type", "judo");
                            data.put("model", modelName);
                            try {
                                data.put("hostAddress", InetAddress.getLocalHost().getHostAddress());
                                data.put("hostName", InetAddress.getLocalHost().getHostName());
                            } catch (Exception e) {
                                // nothing to do
                            }
                            data.put("metric", entry.getKey());
                            data.put("count", entry.getValue().longValue());

                            //final Event event = new Event("decanter/collect/judo", data);
                            //eventAdmin.postEvent(event);
                        });
            } */
            if (metricsLogger.isDebugEnabled()) {
                metricsLogger.debug(metrics.entrySet().stream()
                        .filter(e -> verbose || !e.getKey().startsWith("_"))
                        .map(e -> e.getKey() + ":" + e.getValue())
                        .collect(Collectors.joining(",")));
            }
        }
    }

    @RequiredArgsConstructor
    @ToString
    static final class StackEntry {
        @NonNull
        private final String key;
        private final long startTimestamp = System.nanoTime();
        private final AtomicLong nested = new AtomicLong(0);
    }
}
