package hu.blackbelt.judo.runtime.core.dispatcher.environment;

import lombok.NoArgsConstructor;

import java.util.function.Function;

@NoArgsConstructor
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
