package hu.blackbelt.judo.services.dispatcher.environment;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import java.util.function.Function;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE, property = {
        "judo.environment-variable-provider=true",
        "category=ENVIRONMENT"
})
public class EnvironmentVariableProvider implements Function<String, Object> {

    @Override
    public Object apply(final String key) {
        final String value;
        if (System.getenv().containsKey(key)) {
            value = System.getenv(key);
        } else {
            value = System.getProperty(key);
        }

        return value;
    }
}
