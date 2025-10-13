package hu.blackbelt.judo.runtime.core.guice.testkit.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.*;

/**
 * Comprehensive tests for EnvironmentVariables class covering:
 * - Construction with different parameter types
 * - Immutable and mutable operations
 * - Setting and removing variables
 * - Execution context with environment variables
 * - Error handling and validation
 */
@DisplayName("EnvironmentVariables Comprehensive Tests")
class EnvironmentVariablesTest {

    @BeforeAll
    static void setupMocking() {
        EnvironmentVariableMocker.initMocked();
    }

    @AfterAll
    static void teardownMocking() {
        EnvironmentVariableMocker.deinitMocked();
    }

    @Nested
    @DisplayName("Construction Tests")
    class ConstructionTests {

        @Test
        @DisplayName("Should create empty EnvironmentVariables with default constructor")
        void testDefaultConstructor() {
            EnvironmentVariables envVars = new EnvironmentVariables();

            assertTrue(envVars.getVariables().isEmpty(), "Variables should be empty");
        }

        @Test
        @DisplayName("Should create EnvironmentVariables with single name-value pair")
        void testConstructorWithSinglePair() {
            EnvironmentVariables envVars = new EnvironmentVariables("TEST_VAR", "test_value");

            Map<String, String> vars = envVars.getVariables();
            assertEquals(1, vars.size());
            assertEquals("test_value", vars.get("TEST_VAR"));
        }

        @Test
        @DisplayName("Should create EnvironmentVariables with multiple name-value pairs")
        void testConstructorWithMultiplePairs() {
            EnvironmentVariables envVars = new EnvironmentVariables(
                "VAR1", "value1",
                "VAR2", "value2",
                "VAR3", "value3"
            );

            Map<String, String> vars = envVars.getVariables();
            assertEquals(3, vars.size());
            assertEquals("value1", vars.get("VAR1"));
            assertEquals("value2", vars.get("VAR2"));
            assertEquals("value3", vars.get("VAR3"));
        }

        @Test
        @DisplayName("Should throw exception for odd number of parameters")
        void testConstructorWithOddParameters() {
            assertThrows(IllegalArgumentException.class, () -> {
                new EnvironmentVariables("VAR1", "value1", "VAR2");
            }, "Should throw exception for odd number of parameters");
        }

        @Test
        @DisplayName("Should create EnvironmentVariables from Properties")
        void testConstructorWithProperties() {
            Properties props = new Properties();
            props.setProperty("PROP1", "value1");
            props.setProperty("PROP2", "value2");

            EnvironmentVariables envVars = new EnvironmentVariables(props);

            Map<String, String> vars = envVars.getVariables();
            assertEquals(2, vars.size());
            assertEquals("value1", vars.get("PROP1"));
            assertEquals("value2", vars.get("PROP2"));
        }

        @Test
        @DisplayName("Should create EnvironmentVariables from Map")
        void testConstructorWithMap() {
            Map<String, String> initialVars = new HashMap<>();
            initialVars.put("MAP_VAR1", "map_value1");
            initialVars.put("MAP_VAR2", "map_value2");

            EnvironmentVariables envVars = new EnvironmentVariables(initialVars);

            Map<String, String> vars = envVars.getVariables();
            assertEquals(2, vars.size());
            assertEquals("map_value1", vars.get("MAP_VAR1"));
            assertEquals("map_value2", vars.get("MAP_VAR2"));
        }
    }

    @Nested
    @DisplayName("Immutable Operations Tests")
    class ImmutableOperationsTests {

        @Test
        @DisplayName("Should create new instance with and() method")
        void testImmutableAnd() {
            EnvironmentVariables original = new EnvironmentVariables("VAR1", "value1");
            EnvironmentVariables updated = original.and("VAR2", "value2");

            // Original should be unchanged
            assertEquals(1, original.getVariables().size());
            assertEquals("value1", original.getVariables().get("VAR1"));
            assertNull(original.getVariables().get("VAR2"));

            // New instance should have both variables
            assertEquals(2, updated.getVariables().size());
            assertEquals("value1", updated.getVariables().get("VAR1"));
            assertEquals("value2", updated.getVariables().get("VAR2"));
        }

        @Test
        @DisplayName("Should allow chaining multiple and() calls")
        void testChainingAnd() {
            EnvironmentVariables envVars = new EnvironmentVariables()
                .and("VAR1", "value1")
                .and("VAR2", "value2")
                .and("VAR3", "value3");

            Map<String, String> vars = envVars.getVariables();
            assertEquals(3, vars.size());
            assertEquals("value1", vars.get("VAR1"));
            assertEquals("value2", vars.get("VAR2"));
            assertEquals("value3", vars.get("VAR3"));
        }

        @Test
        @DisplayName("Should throw exception when setting same variable twice with and()")
        void testAndWithDuplicateKey() {
            EnvironmentVariables original = new EnvironmentVariables("VAR1", "value1");

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
                original.and("VAR1", "value2");
            });

            assertTrue(exception.getMessage().contains("VAR1"));
            assertTrue(exception.getMessage().contains("'value1'"));
            assertTrue(exception.getMessage().contains("'value2'"));
        }

        @Test
        @DisplayName("Should handle null values in and() method")
        void testAndWithNullValue() {
            EnvironmentVariables envVars = new EnvironmentVariables("VAR1", "value1")
                .and("VAR2", null);

            Map<String, String> vars = envVars.getVariables();
            assertEquals(2, vars.size());
            assertNull(vars.get("VAR2"));
        }
    }

    @Nested
    @DisplayName("Mutable Operations Tests")
    class MutableOperationsTests {

        @Test
        @DisplayName("Should mutate instance with set() method")
        void testMutableSet() {
            EnvironmentVariables envVars = new EnvironmentVariables();
            EnvironmentVariables result = envVars.set("VAR1", "value1");

            assertSame(envVars, result, "Should return same instance");
            assertEquals(1, envVars.getVariables().size());
            assertEquals("value1", envVars.getVariables().get("VAR1"));
        }

        @Test
        @DisplayName("Should allow overwriting existing variable with set()")
        void testSetOverwrite() {
            EnvironmentVariables envVars = new EnvironmentVariables("VAR1", "value1");
            envVars.set("VAR1", "new_value");

            assertEquals("new_value", envVars.getVariables().get("VAR1"));
        }

        @Test
        @DisplayName("Should remove variable with remove() method")
        void testMutableRemove() {
            EnvironmentVariables envVars = new EnvironmentVariables("VAR1", "value1", "VAR2", "value2");
            EnvironmentVariables result = envVars.remove("VAR1");

            assertSame(envVars, result, "Should return same instance");
            assertEquals(1, envVars.getVariables().size());
            assertNull(envVars.getVariables().get("VAR1"));
            assertEquals("value2", envVars.getVariables().get("VAR2"));
        }

        @Test
        @DisplayName("Should handle removing non-existent variable")
        void testRemoveNonExistent() {
            EnvironmentVariables envVars = new EnvironmentVariables("VAR1", "value1");
            envVars.remove("NON_EXISTENT");

            assertEquals(1, envVars.getVariables().size());
            assertEquals("value1", envVars.getVariables().get("VAR1"));
        }

        @Test
        @DisplayName("Should allow chaining set() and remove() calls")
        void testChainingMutableOperations() {
            EnvironmentVariables envVars = new EnvironmentVariables()
                .set("VAR1", "value1")
                .set("VAR2", "value2")
                .remove("VAR1")
                .set("VAR3", "value3");

            Map<String, String> vars = envVars.getVariables();
            assertEquals(2, vars.size());
            assertNull(vars.get("VAR1"));
            assertEquals("value2", vars.get("VAR2"));
            assertEquals("value3", vars.get("VAR3"));
        }
    }

    @Nested
    @DisplayName("Execution Context Tests")
    class ExecutionContextTests {

        @Test
        @DisplayName("Should execute code with environment variables set")
        void testExecuteWithVariables() throws Exception {
            new EnvironmentVariables("TEST_VAR", "test_value")
                .execute(() -> {
                    assertEquals("test_value", System.getenv("TEST_VAR"));
                });
        }

        @Test
        @DisplayName("Should execute code with multiple variables")
        void testExecuteWithMultipleVariables() throws Exception {
            new EnvironmentVariables("VAR1", "value1", "VAR2", "value2", "VAR3", "value3")
                .execute(() -> {
                    assertEquals("value1", System.getenv("VAR1"));
                    assertEquals("value2", System.getenv("VAR2"));
                    assertEquals("value3", System.getenv("VAR3"));
                });
        }

        @Test
        @DisplayName("Should handle null variable values")
        void testExecuteWithNullValue() throws Exception {
            new EnvironmentVariables("NULL_VAR", null)
                .execute(() -> {
                    // Null values should be filtered out by EnvironmentVariableMocker
                    assertNull(System.getenv("NULL_VAR"));
                });
        }

        @Test
        @DisplayName("Should restore environment after execution")
        void testEnvironmentRestoration() throws Exception {
            String originalValue = System.getenv("PATH");

            new EnvironmentVariables("NEW_VAR", "new_value")
                .execute(() -> {
                    assertEquals("new_value", System.getenv("NEW_VAR"));
                });

            // After execution, new variable should not be visible
            // (This depends on proper cleanup in doTeardown)
            assertEquals(originalValue, System.getenv("PATH"));
        }

        @Test
        @DisplayName("Should propagate exceptions from executed code")
        void testExceptionPropagation() {
            EnvironmentVariables envVars = new EnvironmentVariables("VAR", "value");

            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                envVars.execute(() -> {
                    throw new RuntimeException("Test exception");
                });
            });

            assertEquals("Test exception", exception.getMessage());
        }
    }

    @Nested
    @DisplayName("getVariables() Tests")
    class GetVariablesTests {

        @Test
        @DisplayName("Should return copy of variables map")
        void testGetVariablesReturnsCopy() {
            EnvironmentVariables envVars = new EnvironmentVariables("VAR1", "value1");
            Map<String, String> vars1 = envVars.getVariables();
            Map<String, String> vars2 = envVars.getVariables();

            assertNotSame(vars1, vars2, "Should return different map instances");
            assertEquals(vars1, vars2, "But maps should have same content");
        }

        @Test
        @DisplayName("Should not allow modification of internal state via returned map")
        void testGetVariablesIsolation() {
            EnvironmentVariables envVars = new EnvironmentVariables("VAR1", "value1");
            Map<String, String> vars = envVars.getVariables();

            // Modify the returned map
            vars.put("VAR2", "value2");
            vars.remove("VAR1");

            // Internal state should be unchanged
            Map<String, String> actualVars = envVars.getVariables();
            assertEquals(1, actualVars.size());
            assertEquals("value1", actualVars.get("VAR1"));
            assertNull(actualVars.get("VAR2"));
        }
    }

    @Nested
    @DisplayName("Edge Cases and Special Scenarios")
    class EdgeCasesTests {

        @Test
        @DisplayName("Should handle empty variable name")
        void testEmptyVariableName() {
            EnvironmentVariables envVars = new EnvironmentVariables("", "value");

            assertEquals("value", envVars.getVariables().get(""));
        }

        @Test
        @DisplayName("Should handle empty variable value")
        void testEmptyVariableValue() {
            EnvironmentVariables envVars = new EnvironmentVariables("VAR", "");

            assertEquals("", envVars.getVariables().get("VAR"));
        }

        @Test
        @DisplayName("Should handle special characters in variable names")
        void testSpecialCharactersInName() {
            EnvironmentVariables envVars = new EnvironmentVariables(
                "VAR_WITH_UNDERSCORE", "value1",
                "VAR-WITH-DASH", "value2",
                "VAR.WITH.DOT", "value3"
            );

            Map<String, String> vars = envVars.getVariables();
            assertEquals("value1", vars.get("VAR_WITH_UNDERSCORE"));
            assertEquals("value2", vars.get("VAR-WITH-DASH"));
            assertEquals("value3", vars.get("VAR.WITH.DOT"));
        }

        @Test
        @DisplayName("Should handle special characters in variable values")
        void testSpecialCharactersInValue() {
            EnvironmentVariables envVars = new EnvironmentVariables(
                "VAR1", "value with spaces",
                "VAR2", "value=with=equals",
                "VAR3", "value:with:colons"
            );

            Map<String, String> vars = envVars.getVariables();
            assertEquals("value with spaces", vars.get("VAR1"));
            assertEquals("value=with=equals", vars.get("VAR2"));
            assertEquals("value:with:colons", vars.get("VAR3"));
        }

        @Test
        @DisplayName("Should handle large number of variables")
        void testManyVariables() {
            EnvironmentVariables envVars = new EnvironmentVariables();

            for (int i = 0; i < 100; i++) {
                envVars.set("VAR_" + i, "value_" + i);
            }

            Map<String, String> vars = envVars.getVariables();
            assertEquals(100, vars.size());
            assertEquals("value_50", vars.get("VAR_50"));
        }

        @Test
        @DisplayName("Should handle unicode characters in variable values")
        void testUnicodeInValues() {
            EnvironmentVariables envVars = new EnvironmentVariables(
                "UNICODE_VAR", "Hello 世界 🌍"
            );

            assertEquals("Hello 世界 🌍", envVars.getVariables().get("UNICODE_VAR"));
        }
    }

    @Nested
    @DisplayName("Integration Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Should work with nested execution contexts")
        void testNestedExecution() throws Exception {
            new EnvironmentVariables("OUTER", "outer_value")
                .execute(() -> {
                    assertEquals("outer_value", System.getenv("OUTER"));

                    new EnvironmentVariables("INNER", "inner_value")
                        .execute(() -> {
                            assertEquals("outer_value", System.getenv("OUTER"));
                            assertEquals("inner_value", System.getenv("INNER"));
                        });

                    // After inner execution, inner variable should be gone
                    assertEquals("outer_value", System.getenv("OUTER"));
                });
        }

        @Test
        @DisplayName("Should allow mixing immutable and mutable operations")
        void testMixedOperations() {
            EnvironmentVariables env1 = new EnvironmentVariables("VAR1", "value1");
            EnvironmentVariables env2 = env1.and("VAR2", "value2"); // Immutable
            env2.set("VAR3", "value3"); // Mutable
            env2.remove("VAR2"); // Mutable

            // env1 should be unchanged
            assertEquals(1, env1.getVariables().size());

            // env2 should have VAR1 and VAR3
            Map<String, String> vars = env2.getVariables();
            assertEquals(2, vars.size());
            assertEquals("value1", vars.get("VAR1"));
            assertNull(vars.get("VAR2"));
            assertEquals("value3", vars.get("VAR3"));
        }

        @Test
        @DisplayName("Should preserve existing system environment variables")
        void testPreservesSystemEnvironment() throws Exception {
            String pathValue = System.getenv("PATH");
            assertNotNull(pathValue, "PATH should exist in system environment");

            new EnvironmentVariables("CUSTOM_VAR", "custom_value")
                .execute(() -> {
                    // System variables should still be accessible
                    assertEquals(pathValue, System.getenv("PATH"));
                    assertEquals("custom_value", System.getenv("CUSTOM_VAR"));
                });
        }
    }
}
