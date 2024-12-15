package hu.blackbelt.judo.runtime.core.util;

import hu.blackbelt.judo.runtime.core.utils.EnvironmentVariableResolver;
import hu.blackbelt.judo.runtime.core.utils.RuntimeVariableResolver;
import hu.blackbelt.judo.runtime.core.utils.PropertyFileVariableResolver;
import hu.blackbelt.judo.runtime.core.utils.SystemPropertiesVariableResolver;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static uk.org.webcompere.systemstubs.SystemStubs.restoreSystemProperties;
import static uk.org.webcompere.systemstubs.SystemStubs.withEnvironmentVariable;

class RuntimeVariableResolverTest {

    public static final String CAMEL_CASE = "camelCase";
    public static final String DOT_SEPARATED = "dot separated";
    public static final String ENV = "env";
    public static final String WRONG = "wrong";
    public static final String PROPERTYFILE = "judoPidNamePropertyFile";

    @Test
    void testVariableNaming() throws Exception {
        restoreSystemProperties(() -> {
            withEnvironmentVariable("JUDO_PID_NAME_ENV", ENV)
                    .execute(() -> {
                        System.setProperty("judoPidNameCamel", CAMEL_CASE);
                        System.setProperty("judo.pid.name.dot", DOT_SEPARATED);
                        RuntimeVariableResolver runtimeVariableResolver = RuntimeVariableResolver.builder()
                                .variableResolvers(Arrays.asList(
                                        new EnvironmentVariableResolver(),
                                        new SystemPropertiesVariableResolver(),
                                        PropertyFileVariableResolver.builder()
                                                .parameterDirectory(new File(this.getClass().getClassLoader().getResource("./").getFile()))
                                                .parameterFiles(List.of(new File("testVariableNaming.properties")))
                                                .build()
                                ))
                                .build();
                        assertEquals(CAMEL_CASE, runtimeVariableResolver.getVariableAsString("judoPidNameCamel", WRONG));
                        assertEquals(DOT_SEPARATED, runtimeVariableResolver.getVariableAsString("judoPidNameDot", WRONG));
                        assertEquals(ENV, runtimeVariableResolver.getVariableAsString("judoPidNameEnv", WRONG));
                        assertEquals(PROPERTYFILE, runtimeVariableResolver.getVariableAsString("judoPidNamePropertyFile", WRONG));
                        assertEquals(WRONG, runtimeVariableResolver.getVariableAsString("judoPidDoesNotExists", WRONG));
                    });
        });
    }

    @Test
    void testPrefix() throws Exception {
        restoreSystemProperties(() -> {
            withEnvironmentVariable("JUDO_PREFIX_VARIABLE", "value")
                    .execute(() -> {
                        RuntimeVariableResolver runtimeVariableResolver = RuntimeVariableResolver.builder()
                                .prefix("judoPrefix")
                                .variablePrecedence(List.of(EnvironmentVariableResolver.ENVIRONMENT_VARIABLES))
                                .variableResolvers(List.of(new EnvironmentVariableResolver()))
                                .build();
                        assertEquals("value", runtimeVariableResolver.getVariableAsString("variable", WRONG));
                    });
        });
    }

    @Test
    void testBoolean() throws Exception {
        restoreSystemProperties(() -> {
            withEnvironmentVariable("BOOLEAN_VARIABLE", "True")
                    .execute(() -> {
                        RuntimeVariableResolver runtimeVariableResolver = RuntimeVariableResolver.builder()
                                .variablePrecedence(List.of(EnvironmentVariableResolver.ENVIRONMENT_VARIABLES))
                                .variableResolvers(List.of(new EnvironmentVariableResolver()))
                                .build();
                        assertEquals(true, runtimeVariableResolver.getVariableAsBoolean("booleanVariable", false));
                    });
        });
    }

    @Test
    void testInteger() throws Exception {
        restoreSystemProperties(() -> {
            withEnvironmentVariable("INTEGER_VARIABLE", "134")
                    .execute(() -> {
                        RuntimeVariableResolver runtimeVariableResolver = RuntimeVariableResolver.builder()
                                .variablePrecedence(List.of(EnvironmentVariableResolver.ENVIRONMENT_VARIABLES))
                                .variableResolvers(List.of(new EnvironmentVariableResolver()))
                                .build();
                        assertEquals(134, runtimeVariableResolver.getVariableAsInteger("integerVariable", -1));
                    });
        });
    }


    @Test
    void testBooleanVariable() throws Exception {
        restoreSystemProperties(() -> {
            withEnvironmentVariable("JUDO_PID_NAME_ENV", ENV)
                    .execute(() -> {
                        System.setProperty("judoPidNameCamel", CAMEL_CASE);
                        System.setProperty("judo.pid.name.dot", DOT_SEPARATED);
                        RuntimeVariableResolver runtimeVariableResolver = RuntimeVariableResolver.builder()
                                .variableResolvers(Arrays.asList(
                                        new EnvironmentVariableResolver(),
                                        new SystemPropertiesVariableResolver(),
                                        PropertyFileVariableResolver.builder()
                                                .parameterDirectory(new File(this.getClass().getClassLoader().getResource("./").getFile()))
                                                .parameterFiles(List.of(new File("testVariableNaming.properties")))
                                                .build()
                                ))
                                .build();
                        assertEquals(CAMEL_CASE, runtimeVariableResolver.getVariableAsString("judoPidNameCamel", WRONG));
                        assertEquals(DOT_SEPARATED, runtimeVariableResolver.getVariableAsString("judoPidNameDot", WRONG));
                        assertEquals(ENV, runtimeVariableResolver.getVariableAsString("judoPidNameEnv", WRONG));
                        assertEquals(PROPERTYFILE, runtimeVariableResolver.getVariableAsString("judoPidNamePropertyFile", WRONG));
                        assertEquals(WRONG, runtimeVariableResolver.getVariableAsString("judoPidDoesNotExists", WRONG));
                    });
        });
    }

    @Test
    void testVariablePrecedence() throws Exception {
        restoreSystemProperties(() -> {
            withEnvironmentVariable("JUDO_PID_NAME", ENV)
                    .execute(() -> {
                        System.setProperty("judoPidName", CAMEL_CASE);
                        RuntimeVariableResolver runtimeVariableResolver = RuntimeVariableResolver.builder()
                                .variableResolvers(Arrays.asList(
                                        new EnvironmentVariableResolver(),
                                        new SystemPropertiesVariableResolver(),
                                        PropertyFileVariableResolver.builder()
                                                .parameterDirectory(new File(this.getClass().getClassLoader().getResource("./").getFile()))
                                                .parameterFiles(List.of(new File("testVariablePrecedence.properties")))
                                                .build()
                                ))
                                .variablePrecedence(List.of(SystemPropertiesVariableResolver.SYSTEM_PROPERTIES,
                                        EnvironmentVariableResolver.ENVIRONMENT_VARIABLES))
                                .build();
                        assertEquals(ENV, runtimeVariableResolver.getVariableAsString("judoPidName", WRONG));
                    });
        });

        restoreSystemProperties(() -> {
            withEnvironmentVariable("JUDO_PID_NAME", ENV)
                    .execute(() -> {
                        System.setProperty("judoPidName", CAMEL_CASE);
                        RuntimeVariableResolver runtimeVariableResolver = RuntimeVariableResolver.builder()
                                .variableResolvers(Arrays.asList(
                                        PropertyFileVariableResolver.builder()
                                                .parameterDirectory(new File(this.getClass().getClassLoader().getResource("./").getFile()))
                                                .parameterFiles(List.of(new File("testVariablePrecedence.properties")))
                                                .build()
                                ))
                                .variablePrecedence(List.of(SystemPropertiesVariableResolver.SYSTEM_PROPERTIES,
                                        EnvironmentVariableResolver.ENVIRONMENT_VARIABLES,
                                        PropertyFileVariableResolver.PROPERTIES_FILES))
                                .build();
                        assertEquals(PROPERTYFILE, runtimeVariableResolver.getVariableAsString("judoPidName", WRONG));
                    });
        });

    }

}