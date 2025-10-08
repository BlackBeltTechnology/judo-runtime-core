package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Custom annotation that combines @Test with JUDO runtime setup.
 * Automatically configures the test environment with JUDO runtime fixture and dependency injection.
 *
 * <p>This annotation is part of the JUDO testkit fixture extensions and provides the simplest
 * way to test JUDO custom implementations (interceptors, operations, etc.).
 *
 * <p>Usage example:
 * <pre>
 * {@code
 * @JudoTest
 * void testMyInterceptor(JudoRuntimeFixture fixture) {
 *     MyInterceptor interceptor = ReferenceInjector.createAndInject(
 *         MyInterceptor.class,
 *         fixture.getInjector()
 *     );
 *
 *     // Test your logic
 * }
 * }
 * </pre>
 *
 * @see JudoTestExtension
 * @see JudoRuntimeExtension
 */
@Target({ ElementType.METHOD, ElementType.TYPE, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Test
@ExtendWith(JudoTestExtension.class)
public @interface JudoTest {
    /**
     * Model name to use for the test. Defaults to "example".
     */
    String modelName() default "example";

    /**
     * Database dialect to use. Defaults to "hsqldb".
     * Valid values: "hsqldb", "postgresql"
     *
     * <p>Can be overridden by environment variable {@code JUDO_TEST_DIALECT}.
     *
     * <p><b>Configuration Priority:</b></p>
     * <ol>
     * <li>Environment variable {@code JUDO_TEST_DIALECT} (highest priority)</li>
     * <li>Annotation parameter {@code dialect}</li>
     * <li>Default value: "hsqldb"</li>
     * </ol>
     *
     * <p>Example:
     * <pre>
     * # Override all tests to use PostgreSQL
     * export JUDO_TEST_DIALECT=postgresql
     * mvn test
     * </pre>
     */
    String dialect() default "hsqldb";

    /**
     * Database container to use for testing. Defaults to "none".
     * Valid values: "none", "postgresql", "yugabytedb"
     *
     * <p>Can be overridden by environment variable {@code JUDO_TEST_CONTAINER}.
     *
     * <p><b>Configuration Priority:</b></p>
     * <ol>
     * <li>Environment variable {@code JUDO_TEST_CONTAINER} (highest priority)</li>
     * <li>Annotation parameter {@code container}</li>
     * <li>Auto-detection: if {@code dialect=postgresql} and {@code container=none}, uses "postgresql"</li>
     * <li>Default value: "none" (in-memory database)</li>
     * </ol>
     *
     * <p><b>Container Behavior:</b></p>
     * <ul>
     * <li>"none": Uses in-memory HSQLDB (no container)</li>
     * <li>"postgresql": Uses official PostgreSQL testcontainer</li>
     * <li>"yugabytedb": Uses YugabyteDB testcontainer (PostgreSQL-compatible)</li>
     * </ul>
     *
     * <p><b>Auto-detection:</b> When {@code dialect=postgresql} and no container is specified,
     * automatically uses PostgreSQL container for convenience.
     *
     * <p>Example:
     * <pre>
     * # Override all tests to use YugabyteDB
     * export JUDO_TEST_DIALECT=postgresql
     * export JUDO_TEST_CONTAINER=yugabytedb
     * mvn test
     * </pre>
     */
    String container() default "none";

    /**
     * Transaction handling strategy.
     *
     * <ul>
     * <li>AUTO_ROLLBACK (default): Transaction is automatically started and rolled back after test</li>
     * <li>AUTO_COMMIT: Transaction is automatically started and committed after test</li>
     * <li>MANUAL: No automatic transaction handling - you control begin/commit/rollback</li>
     * <li>NONE: No transaction support</li>
     * </ul>
     */
    TransactionHandling transaction() default TransactionHandling.AUTO_ROLLBACK;

    /**
     * Whether to truncate tables after each test.
     * Only applies when transaction is AUTO_COMMIT or MANUAL.
     * Default: true
     */
    boolean truncateTables() default true;

    /**
     * Model loading strategy.
     *
     * <ul>
     * <li>AUTO (default): Try filesystem first, fallback to classpath</li>
     * <li>FILESYSTEM: Load only from filesystem (target/generated-test-sources/model)</li>
     * <li>CLASSPATH: Load only from classpath (/model/ directory in JARs)</li>
     * </ul>
     */
    ModelSource modelSource() default ModelSource.AUTO;

    /**
     * Custom Guice modules to install in addition to the default JUDO modules.
     *
     * <p>Use this to provide additional bindings for your tests, such as:
     * <ul>
     * <li>Custom implementations of interfaces</li>
     * <li>Mock objects for external dependencies</li>
     * <li>Test-specific configurations</li>
     * </ul>
     *
     * <p>Example:
     * <pre>
     * {@code
     * @JudoTest(modules = { MyCustomModule.class, TestMockModule.class })
     * void testWithCustomBindings(JudoRuntimeFixture fixture) {
     *     // Your custom modules are loaded
     * }
     * }
     * </pre>
     *
     * <p>Each module class must have a public no-arg constructor.
     */
    Class<? extends com.google.inject.Module>[] modules() default {};

    /**
     * Datasource lifecycle management strategy.
     *
     * <ul>
     * <li>BY_METHOD (default): New datasource for each test method - maximum isolation</li>
     * <li>BY_CLASS: One datasource per test class - shared across all test methods</li>
     * <li>SINGLETON: Single datasource shared across all test classes - fastest but least isolation</li>
     * </ul>
     *
     * <p>Only applies when @JudoTest is used at class level. For method-level annotations,
     * the datasource is always created per method.
     */
    DataSourceMode dataSourceMode() default DataSourceMode.BY_METHOD;

    /**
     * Transaction handling strategies for @JudoTest.
     */
    enum TransactionHandling {
        /**
         * Automatically start transaction before test and rollback after test.
         * This keeps the database clean between tests.
         * Tables are NOT truncated (rollback handles cleanup).
         */
        AUTO_ROLLBACK,

        /**
         * Automatically start transaction before test and commit after test.
         * Changes are persisted to the database.
         * Tables are truncated after test (if truncateTables=true).
         */
        AUTO_COMMIT,

        /**
         * No automatic transaction handling.
         * You must manually call fixture.beginTransaction(), commitTransaction(), or rollbackTransaction().
         * Tables are truncated after test (if truncateTables=true).
         */
        MANUAL,

        /**
         * No transaction support at all.
         * Use this when you don't need transaction management.
         */
        NONE,
    }

    /**
     * Model loading source strategies for @JudoTest.
     */
    enum ModelSource {
        /**
         * Try to load from filesystem first, fallback to classpath if filesystem fails.
         * This is the most flexible option for development and testing.
         */
        AUTO,

        /**
         * Load only from filesystem (target/generated-test-sources/model).
         * Fails if model is not found on filesystem.
         */
        FILESYSTEM,

        /**
         * Load only from classpath (/model/ directory in JARs or test resources).
         * Fails if model is not found on classpath.
         */
        CLASSPATH,
    }

    /**
     * Datasource lifecycle management strategies for @JudoTest.
     */
    enum DataSourceMode {
        /**
         * Create a new datasource for each test method.
         * Provides maximum isolation between tests.
         * Each test method gets a fresh database.
         */
        BY_METHOD,

        /**
         * Create one datasource per test class.
         * Shared across all test methods in the class.
         * Faster than BY_METHOD but less isolation.
         * Useful when tests don't interfere with each other.
         */
        BY_CLASS,

        /**
         * Single datasource shared across all test classes.
         * Best performance for large test suites.
         * Least isolation - requires careful test design.
         * Useful for read-only tests or well-isolated test data.
         */
        SINGLETON,
    }
}
