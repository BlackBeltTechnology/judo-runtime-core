package hu.blackbelt.judo.runtime.core.dispatcher.sequence;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@NoArgsConstructor
@AllArgsConstructor
public class InMemorySequence implements Sequence<Long> {

    private static final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    private Long start = DEFAULT_START;

    private Long increment = DEFAULT_INCREMENT;

    @Override
    public Long getNextValue(final String sequenceName) {
        return increment != null
                ? getSequence(sequenceName).addAndGet(increment)
                : getSequence(sequenceName).incrementAndGet();
    }

    @Override
    public Long getCurrentValue(String sequenceName) {
        return getSequence(sequenceName).get();
    }

    private synchronized AtomicLong getSequence(final String sequenceName) {
        if (sequences.containsKey(sequenceName)) {
            return sequences.get(sequenceName);
        } else {
            final AtomicLong sequence = start != null && increment != null ? new AtomicLong(start - increment) : new AtomicLong();
            sequences.put(sequenceName, sequence);
            return sequence;
        }
    }
}
