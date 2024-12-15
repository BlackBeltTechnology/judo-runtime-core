package hu.blackbelt.judo.runtime.core.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class EnvironmentVariableResolver implements VariableResolver {

    public static final String ENVIRONMENT_VARIABLES = "environmentVariables";

    @Override
    public String getName() {
        return ENVIRONMENT_VARIABLES;
    }

    @Override
    public Map<String, Object> process() {
        return VariableResolver.generalizeTemplateVariableNames(System.getenv().entrySet().stream().collect(
                Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> e.getValue(),
                        (prev, next) -> next, HashMap::new
                )));
    }
}
