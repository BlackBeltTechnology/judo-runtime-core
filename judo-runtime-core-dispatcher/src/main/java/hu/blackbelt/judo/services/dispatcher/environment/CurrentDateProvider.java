package hu.blackbelt.judo.services.dispatcher.environment;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.function.Supplier;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE, property = {
        "judo.environment-variable-provider=true",
        "category=SYSTEM",
        "key=current_date"
})
@Designate(ocd = CurrentDateProvider.Config.class)
public class CurrentDateProvider implements Supplier<LocalDate> {

    @ObjectClassDefinition
    public @interface Config {

        @AttributeDefinition(name = "Zone ID")
        String zoneId();
    }

    private ZoneId zoneId = ZoneId.systemDefault();

    @Activate
    @Modified
    void start(Config config) {
        if (config.zoneId() != null) {
            zoneId = ZoneId.of(config.zoneId());
        }
    }

    @Override
    public LocalDate get() {
        return LocalDate.now(zoneId);
    }
}
