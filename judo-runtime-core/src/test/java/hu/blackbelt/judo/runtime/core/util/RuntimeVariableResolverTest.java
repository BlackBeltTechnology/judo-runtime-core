package hu.blackbelt.judo.runtime.core.util;

import hu.blackbelt.judo.runtime.core.utils.*;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
                        RuntimeVariableResolver runtimeVariableResolver = createResolver(null, null, new File("testVariableNaming.properties"));
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
                        RuntimeVariableResolver runtimeVariableResolver = createResolver(null, "judoPrefix");
                        assertEquals("value", runtimeVariableResolver.getVariableAsString("variable", WRONG));
                    });
        });
    }

    @Test
    void testBoolean() throws Exception {
        restoreSystemProperties(() -> {
            withEnvironmentVariable("BOOLEAN_VARIABLE", "True")
                    .execute(() -> {
                        RuntimeVariableResolver runtimeVariableResolver = createResolver(null, null);
                        assertEquals(true, runtimeVariableResolver.getVariableAsBoolean("booleanVariable", false));
                    });
        });
    }

    @Test
    void testInteger() throws Exception {
        restoreSystemProperties(() -> {
            withEnvironmentVariable("INTEGER_VARIABLE", "134")
                    .execute(() -> {
                        RuntimeVariableResolver runtimeVariableResolver = createResolver(null, null);
                        assertEquals(134, runtimeVariableResolver.getVariableAsInteger("integerVariable", -1));
                    });
        });
    }


    @Test
    void testVariablePrecedence() throws Exception {
        restoreSystemProperties(() -> {
            withEnvironmentVariable("JUDO_PID_NAME", ENV)
                    .execute(() -> {
                        System.setProperty("judoPidName", CAMEL_CASE);
                        RuntimeVariableResolver runtimeVariableResolver = createResolver(
                                List.of(SystemPropertiesVariableResolver.SYSTEM_PROPERTIES,
                                        EnvironmentVariableResolver.ENVIRONMENT_VARIABLES),
                                null, new File("testVariablePrecedence.properties"));

                        assertEquals(ENV, runtimeVariableResolver.getVariableAsString("judoPidName", WRONG));
                    });
        });

        restoreSystemProperties(() -> {
            withEnvironmentVariable("JUDO_PID_NAME", ENV)
                    .execute(() -> {
                        System.setProperty("judoPidName", CAMEL_CASE);
                        RuntimeVariableResolver runtimeVariableResolver = createResolver(
                                List.of(SystemPropertiesVariableResolver.SYSTEM_PROPERTIES, EnvironmentVariableResolver.ENVIRONMENT_VARIABLES, PropertyFileVariableResolver.PROPERTIES_FILES),
                                null, new File("testVariablePrecedence.properties"));

                        assertEquals(PROPERTYFILE, runtimeVariableResolver.getVariableAsString("judoPidName", WRONG));
                    });
        });

    }

    private RuntimeVariableResolver createResolver(List<String> precedence, String prefix, File... propertyFiles) {
        RuntimeVariableResolver.RuntimeVariableResolverBuilder builder = RuntimeVariableResolver.builder();

        if (precedence != null) {
            builder.variablePrecedence(precedence);
        }

        if (prefix != null) {
            builder.prefix(prefix);
        }

        Collection<VariableResolver> resolvers = new ArrayList<>();
        resolvers.add(new EnvironmentVariableResolver());
        resolvers.add(new SystemPropertiesVariableResolver());

        if (propertyFiles != null && propertyFiles.length > 0) {
            resolvers.add(PropertyFileVariableResolver.builder()
                    .parameterDirectory(new File(getClass().getClassLoader().getResource("./").getFile()))
                    .parameterFiles(Arrays.asList(propertyFiles))
                    .build());
        }

        return builder.variableResolvers(resolvers).build();
    }
}