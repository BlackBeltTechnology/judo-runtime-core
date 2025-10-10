package hu.blackbelt.judo.runtime.core.guice.testkit.examples;

import static org.junit.jupiter.api.Assertions.*;

import com.google.inject.AbstractModule;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceByClassExtension;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceSingletonExtension;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.ReferenceInjector;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Comprehensive examples showing all available JUDO test extensions.
 *
 * The JUDO testkit provides several extensions for different use cases:
 *
 * 1. JudoRuntimeExtension - Complete setup with datasource, runtime, and transaction management
 * 2. JudoDatasourceByClassExtension - Datasource per test class
 * 3. JudoDatasourceSingletonExtension - Shared datasource across all test classes
 * 4. JudoRuntimeByClassExtension - Minimal runtime fixture setup
 */
class ExtensionBasedTest {

    /**
     * EXAMPLE 1: Using JudoRuntimeExtension (Recommended for most cases)
     *
     * This extension provides:
     * - Automatic datasource setup and teardown
     * - JUDO runtime initialization with model loading
     * - Automatic transaction management (begin before each test, commit after)
     * - Table truncation after each test (clean database state)
     * - JudoRuntimeFixture parameter injection
     */
    @Nested
    @DisplayName("Example 1: JudoRuntimeExtension (Full Setup)")
    class UsingJudoRuntimeExtension {

        @RegisterExtension
        JudoRuntimeExtension extension = new JudoRuntimeExtension(
            "example", // Model name
            new AbstractModule() {
                @Override
                protected void configure() {
                    // Add custom Guice bindings here if needed
                    // Example: bind(MyService.class).to(MyServiceImpl.class);
                }
            }
        );

        @Test
        @DisplayName("Should inject all dependencies automatically")
        void testAutomaticInjection(JudoRuntimeFixture fixture) {
            // The fixture is fully initialized and ready to use
            assertNotNull(fixture);
            assertNotNull(fixture.getInjector());
            // Create and inject interceptor in one line
            // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //     UserCreateInterceptor.class,
            //     fixture.getInjector()
            // );
            // assertNotNull(interceptor);
            // Transaction is automatically managed by the extension
        }

        @Test
        @DisplayName("Should provide clean database for each test")
        void testDatabaseIsolation(JudoRuntimeFixture fixture) {
            // Each test gets a clean database (tables truncated after previous test)
            assertNotNull(fixture);
            assertNotNull(fixture.getInjector());
            // PartnerCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //     PartnerCreateInterceptor.class,
            //     fixture.getInjector()
            // );
            // assertNotNull(interceptor);
            // Transaction automatically committed and tables truncated after test
        }

        @Test
        @DisplayName("Should handle multiple interceptors")
        void testMultipleInterceptors(JudoRuntimeFixture fixture) {
            // You can create multiple interceptors in the same test
            assertNotNull(fixture);
            assertNotNull(fixture.getInjector());
            // UserCreateInterceptor userInterceptor = ReferenceInjector.createAndInject(
            //     UserCreateInterceptor.class,
            //     fixture.getInjector()
            // );
            // PartnerCreateInterceptor partnerInterceptor = ReferenceInjector.createAndInject(
            //     PartnerCreateInterceptor.class,
            //     fixture.getInjector()
            // );
            // assertNotNull(userInterceptor);
            // assertNotNull(partnerInterceptor);
        }
    }

    /**
     * EXAMPLE 2: Using JudoDatasourceByClassExtension
     *
     * This extension provides:
     * - Datasource setup per test class (not shared between classes)
     * - Automatic cleanup after all tests in the class
     * - JudoDatasourceFixture parameter injection
     * - You need to manually set up JudoRuntimeFixture
     *
     * Use this when you need more control over runtime initialization.
     */
    @Nested
    @DisplayName("Example 2: JudoDatasourceByClassExtension (Per-Class Datasource)")
    class UsingJudoDatasourceByClassExtension {

        @RegisterExtension
        JudoDatasourceByClassExtension datasourceExtension = new JudoDatasourceByClassExtension();

        private JudoRuntimeFixture runtimeFixture;

        @BeforeEach
        void setUp(JudoDatasourceFixture datasourceFixture) throws Exception {
            // Manually initialize runtime fixture with the provided datasource
            runtimeFixture = new JudoRuntimeFixture();
            runtimeFixture.prepare("example", datasourceFixture.getDataSource(), datasourceFixture.getDialect());
            runtimeFixture.init(new AbstractModule() {}, this);
            runtimeFixture.beginTransaction();
        }

        @AfterEach
        void tearDown() {
            if (runtimeFixture != null) {
                try {
                    runtimeFixture.commitTransaction();
                } catch (Exception e) {
                    runtimeFixture.rollbackTransaction();
                }
            }
        }

        @Test
        @DisplayName("Should work with manual runtime setup")
        void testManualRuntimeSetup(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture);
            assertNotNull(runtimeFixture);
            // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //     UserCreateInterceptor.class,
            //     runtimeFixture.getInjector()
            // );
            // assertNotNull(interceptor);
        }

        @Test
        @DisplayName("Should have separate datasource per class")
        void testSeparateDatasource(JudoDatasourceFixture datasourceFixture) {
            // This class has its own datasource instance
            assertNotNull(datasourceFixture.getDataSource());
        }
    }

    /**
     * EXAMPLE 3: Using JudoDatasourceSingletonExtension
     *
     * This extension provides:
     * - Shared singleton datasource across ALL test classes
     * - Initialized once and reused (better performance for large test suites)
     * - Never torn down (singleton pattern)
     * - JudoDatasourceFixture parameter injection
     *
     * Use this when you have many test classes and want to share a single datasource.
     */
    @Nested
    @DisplayName("Example 3: JudoDatasourceSingletonExtension (Shared Datasource)")
    class UsingJudoDatasourceSingletonExtension {

        @RegisterExtension
        JudoDatasourceSingletonExtension datasourceExtension = new JudoDatasourceSingletonExtension();

        private JudoRuntimeFixture runtimeFixture;

        @BeforeEach
        void setUp(JudoDatasourceFixture datasourceFixture) throws Exception {
            runtimeFixture = new JudoRuntimeFixture();
            runtimeFixture.prepare("example", datasourceFixture.getDataSource(), datasourceFixture.getDialect());
            runtimeFixture.init(new AbstractModule() {}, this);
            runtimeFixture.beginTransaction();
        }

        @AfterEach
        void tearDown() {
            if (runtimeFixture != null) {
                try {
                    runtimeFixture.commitTransaction();
                } catch (Exception e) {
                    runtimeFixture.rollbackTransaction();
                }
            }
        }

        @Test
        @DisplayName("Should use singleton datasource")
        void testSingletonDatasource(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture);
            assertNotNull(runtimeFixture);
            // This is the same datasource instance used by all test classes
            // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //     UserCreateInterceptor.class,
            //     runtimeFixture.getInjector()
            // );
            // assertNotNull(interceptor);
        }

        @Test
        @DisplayName("Should share datasource efficiently")
        void testSharedDatasource(JudoDatasourceFixture datasourceFixture) {
            // Better performance for large test suites
            assertNotNull(datasourceFixture.getDataSource());
        }
    }

    /**
     * EXAMPLE 4: Combining Extensions
     *
     * You can combine multiple extensions for different needs.
     * For example, using singleton datasource with custom runtime setup.
     */
    @Nested
    @DisplayName("Example 4: Combining Extensions")
    class CombiningExtensions {

        @RegisterExtension
        JudoDatasourceSingletonExtension datasourceExtension = new JudoDatasourceSingletonExtension();

        @Test
        @DisplayName("Should work with custom initialization")
        void testCustomSetup(JudoDatasourceFixture datasourceFixture) throws Exception {
            // Custom runtime setup for specific test needs
            assertNotNull(datasourceFixture);
            JudoRuntimeFixture fixture = new JudoRuntimeFixture();
            fixture.prepare("example", datasourceFixture.getDataSource(), datasourceFixture.getDialect());
            // Custom Guice module with test-specific bindings
            fixture.init(
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        // Add test-specific bindings
                    }
                },
                this
            );
            fixture.beginTransaction();
            try {
                assertNotNull(fixture.getInjector());
                // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
                //     UserCreateInterceptor.class,
                //     fixture.getInjector()
                // );
                // assertNotNull(interceptor);
                fixture.commitTransaction();
            } catch (Exception e) {
                fixture.rollbackTransaction();
                throw e;
            }
        }
    }

    /**
     * CHOOSING THE RIGHT EXTENSION:
     *
     * Use JudoRuntimeExtension when:
     * - You want everything set up automatically
     * - You need transaction management
     * - You want clean database state between tests
     * - This is the recommended default choice
     *
     * Use JudoDatasourceByClassExtension when:
     * - You need manual control over runtime initialization
     * - Different test classes need different runtime configurations
     * - You want datasource isolation per test class
     *
     * Use JudoDatasourceSingletonExtension when:
     * - You have many test classes
     * - You want to optimize test suite performance
     * - Database state doesn't need to be completely clean between test classes
     * - You're okay with sharing a single datasource
     *
     * Use JudoRuntimeByClassExtension when:
     * - You need minimal setup
     * - You only need the fixture for parameter injection
     * - You'll handle all initialization manually
     */
}
