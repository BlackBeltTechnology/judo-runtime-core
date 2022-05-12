package hu.blackbelt.judo.runtime.core.dispatcher;

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.MetricsCancelToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DefaultMetricsCollectorTest {

    private DefaultMetricsCollector metricsCollector;
    private final String outerKey = "OUTER";
    private final String innerKey = "INNER";

    @Mock
    private Context context;

//    @Mock
//    private EventAdmin eventAdmin;

    private Stack<DefaultMetricsCollector.StackEntry> stack;
    private Map<String, AtomicLong> measurements;

    @BeforeEach
    void setUp() {
        context = mock(Context.class);
//        eventAdmin = mock(EventAdmin.class);
        metricsCollector = DefaultMetricsCollector.builder()
                .enabled(true)
                .context(context)
                .build();
//        metricsCollector.setEventAdmin(eventAdmin);
        stack = new Stack<>();
        measurements = new TreeMap<>();
        Mockito.lenient().when(context.getAs(eq(Stack.class), anyString())).thenReturn(stack);
        Mockito.lenient().when(context.getAs(eq(Map.class), anyString())).thenReturn(measurements);
    }

    @Test
    void startingAddsStackEntry() {
        assertTrue(stack.empty());
        try (MetricsCancelToken ct_outer = metricsCollector.start(outerKey)) {
            assertFalse(stack.empty());
        }
    }

    @Test
    void stoppingRemovesStackEntryAndAddsMapEntry() {
        try (MetricsCancelToken ct_outer = metricsCollector.start(outerKey)) {
            assertFalse(stack.empty());
            assertTrue(measurements.isEmpty());
        }
        assertTrue(stack.empty());
        assertFalse(measurements.isEmpty());
    }

    @Test
    void nestedAutomaticClosingDoesNotThrowAndEmptiesStack() {
        assertDoesNotThrow(() -> {
            try (MetricsCancelToken ct_outer = metricsCollector.start(outerKey)) {
                try (MetricsCancelToken ct_inner = metricsCollector.start(innerKey)) {
                    assertEquals(2, stack.size());
                }
                assertEquals(1, stack.size());
            }
            assertEquals(0, stack.size());
        });
    }

    @Test
    void closingPrematurelyThrows() {
        assertThrows(IllegalStateException.class, () -> {
            try (MetricsCancelToken ct_outer = metricsCollector.start(outerKey)) {
                try (MetricsCancelToken ct_inner = metricsCollector.start(innerKey)) {
                    ct_outer.close();
                }
            }
        });
    }

    @Test
    void stoppingPrematurelyThrows() {
        assertThrows(IllegalStateException.class, () -> {
            try (MetricsCancelToken ct_outer = metricsCollector.start(outerKey)) {
                try (MetricsCancelToken ct_inner = metricsCollector.start(innerKey)) {
                    metricsCollector.stop(outerKey);
                }
            }
        });
    }

    @Test
    void doubleStopThrows() {
        assertThrows(IllegalStateException.class, () -> {
            metricsCollector.start(outerKey);
            metricsCollector.start(innerKey);
            metricsCollector.stop(innerKey);
            metricsCollector.stop(innerKey);
        });
    }

    @Test
    void autoCloseSubmits() throws InterruptedException {
        //metricsCollector.setVerbose(true);
        try (MetricsCancelToken ct = metricsCollector.start(outerKey)) {
            Thread.sleep(1L);
        }
        //verify(eventAdmin).postEvent(any(Event.class));
    }

    @Test
    void measurementIsNotZero() throws InterruptedException {
        try (MetricsCancelToken ct_outer = metricsCollector.start(outerKey)) {
            Thread.sleep(1L);
        }
        assertTrue(measurements.get(outerKey).longValue() > 0L);
    }

    @Test
    void sameKeyIncrementsCounterAgain() throws InterruptedException {
        AtomicLong innerAfterFirstTime, innerAfterSecondTIme;
        try (MetricsCancelToken ct_outer = metricsCollector.start(outerKey)) {
            try (MetricsCancelToken ct_inner = metricsCollector.start(innerKey)) {
                Thread.sleep(1L);
            }
            innerAfterFirstTime = new AtomicLong(measurements.get(innerKey).longValue());
            try (MetricsCancelToken ct_inner = metricsCollector.start(innerKey)) {
                Thread.sleep(1L);
            }
            innerAfterSecondTIme = new AtomicLong(measurements.get(innerKey).longValue());
        }
        assertTrue(innerAfterSecondTIme.longValue() > innerAfterFirstTime.longValue());
    }

}