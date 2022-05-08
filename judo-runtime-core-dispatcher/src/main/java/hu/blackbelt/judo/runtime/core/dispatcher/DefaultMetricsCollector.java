package hu.blackbelt.judo.runtime.core.dispatcher;

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.MetricsCancelToken;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DefaultMetricsCollector implements MetricsCollector {


    private static final Logger metricsLogger = LoggerFactory.getLogger("metrics");
    private static final String FRAMEWORK_METRICS_STACK = FRAMEWORK_METRICS + "_stack";

    @NonNull
    Context context;

    @Builder.Default
    private Boolean enabled = false;

    @Builder.Default
    private Boolean verbose = false;

    @Builder.Default
    Consumer<Map<String, AtomicLong>> metricsConsumer = null;

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
            if (metricsConsumer != null) {
                metricsConsumer.accept(metrics);
            }
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
