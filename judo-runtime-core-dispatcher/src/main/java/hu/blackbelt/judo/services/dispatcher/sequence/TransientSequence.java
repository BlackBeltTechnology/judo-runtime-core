package hu.blackbelt.judo.services.dispatcher.sequence;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.AttributeType;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE, property = {
        Sequence.TYPE_KEY + "=" + TransientSequence.TYPE
})
@Designate(ocd = TransientSequence.Config.class)
@NoArgsConstructor
@AllArgsConstructor
public class TransientSequence implements Sequence<Long> {

    static final String TYPE = "transient";

    @ObjectClassDefinition()
    public @interface Config {

        @AttributeDefinition(name = "start", description = "Start value", type = AttributeType.LONG)
        long start() default 1L;

        @AttributeDefinition(name = "increment", description = "Increment by", type = AttributeType.LONG)
        long increment() default 1L;
    }

    private static final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    private Long start = DEFAULT_START;

    private Long increment = DEFAULT_INCREMENT;

    @Activate
    void start(Config config) {
        start = config.start();
        increment = config.increment();
    }

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
