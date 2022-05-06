package hu.blackbelt.judo.runtime.core.dispatcher.environment;


import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

public class CurrentTimeProvider implements Supplier<LocalTime> {

    private ZoneId zoneId = ZoneId.systemDefault();

    @Override
    public LocalTime get() {
        return LocalTime.now(zoneId).truncatedTo(ChronoUnit.MILLIS);
    }
}
