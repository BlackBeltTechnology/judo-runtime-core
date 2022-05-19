package hu.blackbelt.judo.runtime.core.dispatcher.environment;

import lombok.*;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.function.Supplier;

@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CurrentDateProvider implements Supplier<LocalDate> {

    @Builder.Default
    @Setter
    private ZoneId zoneId = ZoneId.systemDefault();

    @Override
    public LocalDate get() {
        return LocalDate.now(zoneId);
    }
}
