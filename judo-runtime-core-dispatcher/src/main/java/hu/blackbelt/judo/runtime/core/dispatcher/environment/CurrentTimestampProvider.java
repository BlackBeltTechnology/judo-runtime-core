package hu.blackbelt.judo.runtime.core.dispatcher.environment;

import lombok.*;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CurrentTimestampProvider implements Supplier<OffsetDateTime> {

    @Builder.Default
    @Setter
    private ZoneId zoneId = ZoneId.systemDefault();

    @Override
    public OffsetDateTime get() {
        return OffsetDateTime.now(zoneId).truncatedTo(ChronoUnit.MILLIS);
    }
}
