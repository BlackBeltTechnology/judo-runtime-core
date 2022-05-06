package hu.blackbelt.judo.runtime.core.dispatcher.environment;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.function.Supplier;

public class CurrentDateProvider implements Supplier<LocalDate> {

    private ZoneId zoneId = ZoneId.systemDefault();

    @Override
    public LocalDate get() {
        return LocalDate.now(zoneId);
    }
}
