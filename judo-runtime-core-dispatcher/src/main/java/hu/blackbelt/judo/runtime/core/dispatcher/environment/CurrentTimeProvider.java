package hu.blackbelt.judo.runtime.core.dispatcher.environment;


import lombok.*;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CurrentTimeProvider implements Supplier<LocalTime> {

    @Builder.Default
    @Setter
    private ZoneId zoneId = ZoneId.systemDefault();

    @Override
    public LocalTime get() {
        return LocalTime.now(zoneId).truncatedTo(ChronoUnit.MILLIS);
    }
}
