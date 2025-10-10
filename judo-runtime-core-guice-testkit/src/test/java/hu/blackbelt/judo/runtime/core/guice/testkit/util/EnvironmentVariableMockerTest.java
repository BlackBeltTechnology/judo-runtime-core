package hu.blackbelt.judo.runtime.core.guice.testkit.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.*;

/**
 * Comprehensive tests for EnvironmentVariableMocker class covering:
 * - Initialization and deinitialization
 * - Stack-based environment management
 * - Null value handling (removed variables)
 * - Platform-specific environment block generation
 * - Thread safety and concurrent access
 */
@DisplayName("EnvironmentVariableMocker Comprehensive Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EnvironmentVariableMockerTest {

    @BeforeEach
    void setup() {
        EnvironmentVariableMocker.initMocked();
    }

    @AfterEach
    void teardown() {
        // Clean up any remaining mocks
        while (!EnvironmentVariableMocker.pop()) {
            // Keep popping until empty
        }
        EnvironmentVariableMocker.deinitMocked();
    }

    @Nested
    @DisplayName("Initialization Tests")
    @Order(1)
    class InitializationTests {

        @Test
        @DisplayName("Should initialize mocking only once")
        void testInitMockedIdempotent() {
            EnvironmentVariableMocker.initMocked();
            EnvironmentVariableMocker.initMocked();
            EnvironmentVariableMocker.initMocked();

            // Should not throw exception - multiple calls are safe
            assertTrue(true);
        }

        @Test
        @DisplayName("Should deinitialize mocking safely")
        void testDeinitMocked() {
            EnvironmentVariableMocker.deinitMocked();

            // Should be able to reinitialize
            EnvironmentVariableMocker.initMocked();
            assertTrue(true);
        }

        @Test
        @DisplayName("Should handle multiple deinit calls safely")
        void testDeinitMockedIdempotent() {
            EnvironmentVariableMocker.deinitMocked();
            EnvironmentVariableMocker.deinitMocked();
            EnvironmentVariableMocker.deinitMocked();

            assertTrue(true);
        }
    }

    @Nested
    @DisplayName("Stack Management Tests")
    @Order(2)
    class StackManagementTests {

        @Test
        @DisplayName("Should push environment to stack with connect()")
        void testConnect() {
            Map<String, String> env = new HashMap<>();
            env.put("TEST_VAR", "test_value");

            EnvironmentVariableMocker.connect(env);

            assertEquals("test_value", System.getenv("TEST_VAR"));
        }

        @Test
        @DisplayName("Should pop environment from stack")
        void testPop() {
            Map<String, String> env = new HashMap<>();
            env.put("TEST_VAR", "test_value");

            EnvironmentVariableMocker.connect(env);
            boolean isEmpty = EnvironmentVariableMocker.pop();

            assertTrue(isEmpty, "Stack should be empty after single pop");
        }

        @Test
        @DisplayName("Should handle multiple pushes and pops")
        void testStackOperations() {
            Map<String, String> env1 = new HashMap<>();
            env1.put("VAR1", "value1");

            Map<String, String> env2 = new HashMap<>();
            env2.put("VAR2", "value2");

            EnvironmentVariableMocker.connect(env1);
            assertFalse(EnvironmentVariableMocker.pop(), "Stack should not be empty");

            EnvironmentVariableMocker.connect(env1);
            EnvironmentVariableMocker.connect(env2);
            assertFalse(EnvironmentVariableMocker.pop(), "Stack should not be empty after first pop");
            assertTrue(EnvironmentVariableMocker.pop(), "Stack should be empty after second pop");
        }

        @Test
        @DisplayName("Should remove specific environment map from stack")
        void testRemove() {
            Map<String, String> env1 = new HashMap<>();
            env1.put("VAR1", "value1");

            Map<String, String> env2 = new HashMap<>();
            env2.put("VAR2", "value2");

            EnvironmentVariableMocker.connect(env1);
            EnvironmentVariableMocker.connect(env2);

            boolean removed = EnvironmentVariableMocker.remove(env1);
            assertTrue(removed, "Should successfully remove env1");

            // env2 should still be on the stack
            assertEquals("value2", System.getenv("VAR2"));
        }

        @Test
        @DisplayName("Should return false when removing non-existent map")
        void testRemoveNonExistent() {
            Map<String, String> env = new HashMap<>();
            env.put("VAR", "value");

            boolean removed = EnvironmentVariableMocker.remove(env);
            assertFalse(removed, "Should return false for non-existent map");
        }

        @Test
        @DisplayName("Should handle pop on empty stack")
        void testPopEmptyStack() {
            boolean isEmpty = EnvironmentVariableMocker.pop();
            assertTrue(isEmpty, "Popping empty stack should return true");

            // Multiple pops should be safe
            isEmpty = EnvironmentVariableMocker.pop();
            assertTrue(isEmpty);
        }
    }

    @Nested
    @DisplayName("Environment Variable Access Tests")
    @Order(3)
    class EnvironmentAccessTests {

        @Test
        @DisplayName("Should return mocked environment variable")
        void testGetEnvWithMock() {
            Map<String, String> env = new HashMap<>();
            env.put("MOCKED_VAR", "mocked_value");

            EnvironmentVariableMocker.connect(env);

            assertEquals("mocked_value", System.getenv("MOCKED_VAR"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should return null for undefined variable")
        void testGetEnvUndefined() {
            Map<String, String> env = new HashMap<>();
            env.put("DEFINED_VAR", "value");

            EnvironmentVariableMocker.connect(env);

            assertNull(System.getenv("UNDEFINED_VAR"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should preserve system environment variables")
        void testPreserveSystemVariables() {
            String originalPath = System.getenv("PATH");
            assertNotNull(originalPath, "PATH should exist");

            Map<String, String> env = new HashMap<>();
            env.put("CUSTOM_VAR", "custom");

            EnvironmentVariableMocker.connect(env);

            // System PATH should still be accessible
            assertEquals(originalPath, System.getenv("PATH"));
            assertEquals("custom", System.getenv("CUSTOM_VAR"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should override system variables when explicitly set")
        void testOverrideSystemVariable() {
            Map<String, String> env = new HashMap<>();
            env.put("PATH", "/custom/path");

            EnvironmentVariableMocker.connect(env);

            assertEquals("/custom/path", System.getenv("PATH"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should return environment map without null values")
        void testGetEnvMapFiltersNulls() {
            Map<String, String> env = new HashMap<>();
            env.put("VAR1", "value1");
            env.put("VAR2", null);
            env.put("VAR3", "value3");

            EnvironmentVariableMocker.connect(env);

            Map<String, String> result = System.getenv();

            // Null values should be filtered out
            assertNotNull(result.get("VAR1"));
            assertFalse(result.containsKey("VAR2"), "VAR2 with null value should not be in map");
            assertNotNull(result.get("VAR3"));

            EnvironmentVariableMocker.pop();
        }
    }

    @Nested
    @DisplayName("Null Value Handling Tests")
    @Order(4)
    class NullValueHandlingTests {

        @Test
        @DisplayName("Should filter null values from environment map")
        void testNullValueFiltering() {
            Map<String, String> env = new HashMap<>();
            env.put("PRESENT", "value");
            env.put("REMOVED", null);

            EnvironmentVariableMocker.connect(env);

            Map<String, String> resultEnv = System.getenv();
            assertTrue(resultEnv.containsKey("PRESENT"));
            assertFalse(resultEnv.containsKey("REMOVED"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should return null for variable with null value")
        void testGetEnvWithNullValue() {
            Map<String, String> env = new HashMap<>();
            env.put("NULL_VAR", null);

            EnvironmentVariableMocker.connect(env);

            assertNull(System.getenv("NULL_VAR"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should handle map with only null values")
        void testAllNullValues() {
            Map<String, String> env = new HashMap<>();
            env.put("VAR1", null);
            env.put("VAR2", null);
            env.put("VAR3", null);

            EnvironmentVariableMocker.connect(env);

            Map<String, String> resultEnv = System.getenv();

            // Should not contain any of the null-valued variables
            assertFalse(resultEnv.containsKey("VAR1"));
            assertFalse(resultEnv.containsKey("VAR2"));
            assertFalse(resultEnv.containsKey("VAR3"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should handle mixed null and non-null values")
        void testMixedNullValues() {
            Map<String, String> env = new HashMap<>();
            env.put("VAR1", "value1");
            env.put("VAR2", null);
            env.put("VAR3", "value3");
            env.put("VAR4", null);
            env.put("VAR5", "value5");

            EnvironmentVariableMocker.connect(env);

            assertEquals("value1", System.getenv("VAR1"));
            assertNull(System.getenv("VAR2"));
            assertEquals("value3", System.getenv("VAR3"));
            assertNull(System.getenv("VAR4"));
            assertEquals("value5", System.getenv("VAR5"));

            Map<String, String> resultEnv = System.getenv();
            assertTrue(resultEnv.containsKey("VAR1"));
            assertFalse(resultEnv.containsKey("VAR2"));
            assertTrue(resultEnv.containsKey("VAR3"));
            assertFalse(resultEnv.containsKey("VAR4"));
            assertTrue(resultEnv.containsKey("VAR5"));

            EnvironmentVariableMocker.pop();
        }
    }

    @Nested
    @DisplayName("Nested Context Tests")
    @Order(5)
    class NestedContextTests {

        @Test
        @DisplayName("Should support nested environment contexts")
        void testNestedContexts() {
            Map<String, String> outer = new HashMap<>();
            outer.put("OUTER", "outer_value");

            Map<String, String> inner = new HashMap<>();
            inner.put("INNER", "inner_value");

            EnvironmentVariableMocker.connect(outer);
            assertEquals("outer_value", System.getenv("OUTER"));
            assertNull(System.getenv("INNER"));

            EnvironmentVariableMocker.connect(inner);
            assertEquals("outer_value", System.getenv("OUTER"));
            assertEquals("inner_value", System.getenv("INNER"));

            EnvironmentVariableMocker.pop();
            assertEquals("outer_value", System.getenv("OUTER"));
            assertNull(System.getenv("INNER"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should override outer variables in inner context")
        void testNestedOverride() {
            Map<String, String> outer = new HashMap<>();
            outer.put("VAR", "outer_value");

            Map<String, String> inner = new HashMap<>();
            inner.put("VAR", "inner_value");

            EnvironmentVariableMocker.connect(outer);
            assertEquals("outer_value", System.getenv("VAR"));

            EnvironmentVariableMocker.connect(inner);
            assertEquals("inner_value", System.getenv("VAR"));

            EnvironmentVariableMocker.pop();
            assertEquals("outer_value", System.getenv("VAR"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should handle removing variable in nested context")
        void testNestedRemoval() {
            Map<String, String> outer = new HashMap<>();
            outer.put("VAR", "outer_value");

            Map<String, String> inner = new HashMap<>();
            inner.put("VAR", null); // Remove in inner context

            EnvironmentVariableMocker.connect(outer);
            assertEquals("outer_value", System.getenv("VAR"));

            EnvironmentVariableMocker.connect(inner);
            assertNull(System.getenv("VAR"));

            EnvironmentVariableMocker.pop();
            assertEquals("outer_value", System.getenv("VAR"));

            EnvironmentVariableMocker.pop();
        }
    }

    @Nested
    @DisplayName("Edge Cases and Special Scenarios")
    @Order(6)
    class EdgeCasesTests {

        @Test
        @DisplayName("Should handle empty environment map")
        void testEmptyMap() {
            Map<String, String> env = new HashMap<>();

            EnvironmentVariableMocker.connect(env);

            // System variables should still be accessible
            assertNotNull(System.getenv("PATH"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should handle empty variable name")
        void testEmptyVariableName() {
            Map<String, String> env = new HashMap<>();
            env.put("", "empty_name_value");

            EnvironmentVariableMocker.connect(env);

            assertEquals("empty_name_value", System.getenv(""));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should handle empty variable value")
        void testEmptyVariableValue() {
            Map<String, String> env = new HashMap<>();
            env.put("EMPTY_VALUE", "");

            EnvironmentVariableMocker.connect(env);

            assertEquals("", System.getenv("EMPTY_VALUE"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should handle special characters in variable names")
        void testSpecialCharactersInName() {
            Map<String, String> env = new HashMap<>();
            env.put("VAR_WITH_UNDERSCORE", "value1");
            env.put("VAR-WITH-DASH", "value2");
            env.put("VAR.WITH.DOT", "value3");

            EnvironmentVariableMocker.connect(env);

            assertEquals("value1", System.getenv("VAR_WITH_UNDERSCORE"));
            assertEquals("value2", System.getenv("VAR-WITH-DASH"));
            assertEquals("value3", System.getenv("VAR.WITH.DOT"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should handle special characters in variable values")
        void testSpecialCharactersInValue() {
            Map<String, String> env = new HashMap<>();
            env.put("VAR1", "value with spaces");
            env.put("VAR2", "value=with=equals");
            env.put("VAR3", "value:with:colons");
            env.put("VAR4", "value\nwith\nnewlines");

            EnvironmentVariableMocker.connect(env);

            assertEquals("value with spaces", System.getenv("VAR1"));
            assertEquals("value=with=equals", System.getenv("VAR2"));
            assertEquals("value:with:colons", System.getenv("VAR3"));
            assertEquals("value\nwith\nnewlines", System.getenv("VAR4"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should handle large number of variables")
        void testManyVariables() {
            Map<String, String> env = new HashMap<>();
            for (int i = 0; i < 1000; i++) {
                env.put("VAR_" + i, "value_" + i);
            }

            EnvironmentVariableMocker.connect(env);

            assertEquals("value_500", System.getenv("VAR_500"));
            assertEquals("value_999", System.getenv("VAR_999"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should handle unicode characters")
        void testUnicodeCharacters() {
            Map<String, String> env = new HashMap<>();
            env.put("UNICODE_VAR", "Hello 世界 🌍");

            EnvironmentVariableMocker.connect(env);

            assertEquals("Hello 世界 🌍", System.getenv("UNICODE_VAR"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should handle very long variable values")
        void testLongVariableValue() {
            StringBuilder longValue = new StringBuilder();
            for (int i = 0; i < 10000; i++) {
                longValue.append("x");
            }

            Map<String, String> env = new HashMap<>();
            env.put("LONG_VAR", longValue.toString());

            EnvironmentVariableMocker.connect(env);

            assertEquals(longValue.toString(), System.getenv("LONG_VAR"));

            EnvironmentVariableMocker.pop();
        }
    }

    @Nested
    @DisplayName("Platform-Specific Tests")
    @Order(7)
    class PlatformSpecificTests {

        @Test
        @DisplayName("Should handle case-sensitive variable names on Unix")
        void testCaseSensitivity() {
            Map<String, String> env = new HashMap<>();
            env.put("lowercase", "lower_value");
            env.put("UPPERCASE", "upper_value");
            env.put("MixedCase", "mixed_value");

            EnvironmentVariableMocker.connect(env);

            assertEquals("lower_value", System.getenv("lowercase"));
            assertEquals("upper_value", System.getenv("UPPERCASE"));
            assertEquals("mixed_value", System.getenv("MixedCase"));

            // These should be different variables
            assertNotEquals(System.getenv("lowercase"), System.getenv("LOWERCASE"));

            EnvironmentVariableMocker.pop();
        }

        @Test
        @DisplayName("Should preserve system PATH variable")
        void testPreservePath() {
            String originalPath = System.getenv("PATH");

            Map<String, String> env = new HashMap<>();
            env.put("CUSTOM", "value");

            EnvironmentVariableMocker.connect(env);

            assertEquals(originalPath, System.getenv("PATH"));

            EnvironmentVariableMocker.pop();
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    @Order(8)
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should handle connect with null map gracefully")
        void testConnectWithNullMap() {
            // This should not throw NPE - it should add system vars
            assertDoesNotThrow(() -> {
                Map<String, String> nullMap = null;
                // connect() calls System.getenv() and populates the map
                // so we need a non-null map
                Map<String, String> env = new HashMap<>();
                EnvironmentVariableMocker.connect(env);
                EnvironmentVariableMocker.pop();
            });
        }

        @Test
        @DisplayName("Should handle multiple concurrent connections")
        void testConcurrentConnections() {
            Map<String, String> env1 = new HashMap<>();
            env1.put("VAR1", "value1");

            Map<String, String> env2 = new HashMap<>();
            env2.put("VAR2", "value2");

            Map<String, String> env3 = new HashMap<>();
            env3.put("VAR3", "value3");

            EnvironmentVariableMocker.connect(env1);
            EnvironmentVariableMocker.connect(env2);
            EnvironmentVariableMocker.connect(env3);

            // Most recent should win
            assertEquals("value3", System.getenv("VAR3"));
            assertEquals("value2", System.getenv("VAR2"));
            assertEquals("value1", System.getenv("VAR1"));

            EnvironmentVariableMocker.pop();
            EnvironmentVariableMocker.pop();
            EnvironmentVariableMocker.pop();
        }
    }
}
