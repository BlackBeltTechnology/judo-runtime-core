package hu.blackbelt.judo.runtime.core.utils;

import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.impl.DefaultCoercerFactory;
import hu.blackbelt.mapper.impl.DefaultConverterFactory;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.*;

@Slf4j
public class RuntimeVariableResolver {

    public static final String JUDO_PROPERTIES = "judo.properties";
    final List<String> variablePrecedence;
    final String prefix;
    final Collection<VariableResolver> variableResolvers;
    final DefaultCoercerFactory coercerFactory = new DefaultCoercerFactory();
    final Coercer coercer;

    @Getter
    Map<String, Object> variables;

    public static class RuntimeVariableResolverBuilder {
        String prefix = "";
        List<String> variablePrecedence = Arrays.asList(PropertyFileVariableResolver.PROPERTIES_FILES,
                EnvironmentVariableResolver.ENVIRONMENT_VARIABLES,
                SystemPropertiesVariableResolver.SYSTEM_PROPERTIES);
        Collection<VariableResolver> variableResolvers = null;
    }

    @Builder
    public RuntimeVariableResolver(String prefix,
                                   List<String> variablePrecedence,
                                   Collection<VariableResolver> variableResolvers) {
        this.prefix = prefix;
        this.variablePrecedence = variablePrecedence;
        if (variableResolvers == null) {
            this.variableResolvers = Arrays.asList(
                    new SystemPropertiesVariableResolver(),
                    new EnvironmentVariableResolver());
        } else {
            this.variableResolvers = variableResolvers;
        }
        this.coercer = new DefaultCoercerFactory().getCoercerInstance();
        this.variables = getVariables();
    }

    private Map<String, Object> getVariables() {
        Map<String, Object> variables = new LinkedHashMap<>();
        for (String precedence : variablePrecedence) {
            for (VariableResolver variableResolver : variableResolvers) {
                if (precedence.equalsIgnoreCase(variableResolver.getName())) {
                    variables.putAll(variableResolver.process());
                }
            }
        }
        return variables;
    }

    private String getEffectiveVariableName(String name) {
        String variableName = name;
        if (!variableName.startsWith(prefix)) {
            variableName = prefix + VariableResolver.firstToUpper(variableName);
        }
        return variableName;
    }

    public <T> T getVariable(String name, T defaultValue, Class<T> clazz) {
        Object value = variables.get(getEffectiveVariableName(name));
        if (value == null) {
            return defaultValue;
        } else {
            return coercer.coerce(value, clazz);
        }
    }

    public String getVariableAsString(String name, String defaultValue) {
        return getVariable(name, defaultValue, String.class);
    }

    public Boolean getVariableAsBoolean(String name, Boolean defaultValue) {
        return getVariable(name, defaultValue, Boolean.class);
    }

    public Integer getVariableAsInteger(String name, Integer defaultValue) {
        return getVariable(name, defaultValue, Integer.class);
    }

    public Long getVariableAsLong(String name, Long defaultValue) {
        return getVariable(name, defaultValue, Long.class);
    }

    public File getVariableAsFile(String name, File defaultValue) {
        String defaultPath = defaultValue != null ? defaultValue.getPath() : null;
        String path = getVariable(name, defaultPath, String.class);
        return path != null ? new File(path) : null;
    }

}
