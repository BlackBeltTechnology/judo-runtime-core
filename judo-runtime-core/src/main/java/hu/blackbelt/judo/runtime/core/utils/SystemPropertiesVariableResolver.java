package hu.blackbelt.judo.runtime.core.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SystemPropertiesVariableResolver implements VariableResolver {

    public static final String SYSTEM_PROPERTIES = "systemProperties";

    @Override
    public String getName() {
        return SYSTEM_PROPERTIES;
    }

    @Override
    public Map<String, Object> process() {
        return VariableResolver.generalizeTemplateVariableNames(System.getProperties().entrySet().stream().collect(
                Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(e.getValue()),
                        (prev, next) -> next, HashMap::new
                )));
    }
}
