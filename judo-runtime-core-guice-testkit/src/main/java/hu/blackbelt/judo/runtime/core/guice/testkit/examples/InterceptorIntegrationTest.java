package hu.blackbelt.judo.runtime.core.guice.testkit.examples;

import static hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceFixture.DIALECT_HSQLDB;
import static org.junit.jupiter.api.Assertions.*;

import com.google.inject.AbstractModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.ReferenceInjector;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.TestOperationCallInterceptorProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.eclipse.emf.ecore.EOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Example integration tests demonstrating how to test custom interceptors
 * using the JUDO testkit framework.
 *
 * <p>This example shows multiple patterns:
 * <ol>
 * <li>Unit testing: Testing interceptor logic in isolation with ReferenceInjector</li>
 * <li>Integration testing: Testing interceptors within the dispatcher flow</li>
 * <li>Annotation-based: Using @JudoTest(interceptors = {...})</li>
 * <li>Programmatic: Using fixture.addInterceptor()</li>
 * </ol>
 *
 * @see JudoRuntimeFixture#addInterceptor(Class)
 * @see JudoRuntimeFixture#addInterceptor(OperationCallInterceptor)
 * @see JudoTest#interceptors()
 */
class InterceptorIntegrationTest {

    // ==========================================
    // Example 1: Manual Fixture Setup with Programmatic Interceptor Registration
    // ==========================================

    /**
     * A simple spy interceptor for testing purposes.
     * Tracks how many times preCall and postCall were invoked.
     */
    public static class SpyInterceptor implements OperationCallInterceptor {

        private final AtomicInteger preCallCount = new AtomicInteger(0);
        private final AtomicInteger postCallCount = new AtomicInteger(0);
        private EOperation lastOperation;

        @Override
        public String getName() {
            return "SpyInterceptor";
        }

        @Override
        public Collection<EOperation> getOperations(AsmModel asmModel) {
            // Empty means intercept all operations
            return Collections.emptyList();
        }

        @Override
        public Object preCall(EOperation operation, Object parameterPayload) {
            preCallCount.incrementAndGet();
            lastOperation = operation;
            return parameterPayload;
        }

        @Override
        public Object postCall(EOperation operation, Object parameterPayload, Object returnPayload) {
            postCallCount.incrementAndGet();
            return returnPayload;
        }

        public int getPreCallCount() {
            return preCallCount.get();
        }

        public int getPostCallCount() {
            return postCallCount.get();
        }

        public EOperation getLastOperation() {
            return lastOperation;
        }

        public void reset() {
            preCallCount.set(0);
            postCallCount.set(0);
            lastOperation = null;
        }
    }

    private DataSource dataSource;
    private JudoRuntimeFixture runtimeFixture;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize HSQLDB in-memory datasource
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:test");
        config.setUsername("SA");
        config.setPassword("");
        config.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(config);
    }

    @AfterEach
    void tearDown() {
        if (runtimeFixture != null) {
            runtimeFixture.tearDown();
        }
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }

    /**
     * Example: Register interceptor by class (programmatic).
     *
     * <p>The interceptor class is instantiated and its dependencies are
     * automatically injected after the Guice injector is created.
     */
    @Test
    void testRegisterInterceptorByClass() throws Exception {
        // Initialize runtime fixture
        runtimeFixture = new JudoRuntimeFixture();
        runtimeFixture.prepare("exa", dataSource, DIALECT_HSQLDB);

        // Register interceptor class BEFORE init()
        runtimeFixture.addInterceptor(SpyInterceptor.class);

        // Initialize - interceptor is instantiated and dependencies injected
        runtimeFixture.init(new AbstractModule() {}, null);

        // Verify interceptor is registered
        TestOperationCallInterceptorProvider provider = runtimeFixture.getInterceptorProvider();
        assertNotNull(provider);
        assertEquals(1, provider.size());
        assertTrue(provider.getInterceptors().get(0) instanceof SpyInterceptor);
    }

    /**
     * Example: Register pre-configured interceptor instance (programmatic).
     *
     * <p>Useful when your interceptor requires constructor parameters
     * or custom configuration before injection.
     */
    @Test
    void testRegisterInterceptorByInstance() throws Exception {
        // Create a pre-configured interceptor
        SpyInterceptor spy = new SpyInterceptor();

        // Initialize runtime fixture
        runtimeFixture = new JudoRuntimeFixture();
        runtimeFixture.prepare("exa", dataSource, DIALECT_HSQLDB);

        // Register the instance BEFORE init()
        runtimeFixture.addInterceptor(spy);

        // Initialize - dependencies are injected into the instance
        runtimeFixture.init(new AbstractModule() {}, null);

        // Verify the same instance is registered
        TestOperationCallInterceptorProvider provider = runtimeFixture.getInterceptorProvider();
        assertSame(spy, provider.getInterceptors().get(0));
    }

    /**
     * Example: Access interceptor provider for advanced manipulation.
     *
     * <p>The provider can be used to add, remove, or clear interceptors
     * at runtime during a test.
     */
    @Test
    void testInterceptorProviderManipulation() throws Exception {
        // Initialize runtime fixture
        runtimeFixture = new JudoRuntimeFixture();
        runtimeFixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        runtimeFixture.addInterceptor(SpyInterceptor.class);
        runtimeFixture.init(new AbstractModule() {}, null);

        // Get the provider
        TestOperationCallInterceptorProvider provider = runtimeFixture.getInterceptorProvider();

        // Verify initial state
        assertEquals(1, provider.size());

        // Clear all interceptors
        provider.clearInterceptors();
        assertEquals(0, provider.size());
        assertTrue(provider.isEmpty());

        // Add a new interceptor at runtime
        SpyInterceptor newSpy = new SpyInterceptor();
        provider.addInterceptor(newSpy);
        assertEquals(1, provider.size());

        // Remove the interceptor
        assertTrue(provider.removeInterceptor(newSpy));
        assertEquals(0, provider.size());
    }

    // ==========================================
    // Example 2: Unit Testing Interceptor Logic in Isolation
    // ==========================================

    /**
     * Example: Test interceptor logic in isolation using ReferenceInjector.
     *
     * <p>This pattern is useful when you want to test the interceptor's
     * preCall/postCall logic directly without going through the dispatcher.
     */
    @Test
    void testInterceptorLogicInIsolation() throws Exception {
        // Initialize runtime fixture (for dependency injection only)
        runtimeFixture = new JudoRuntimeFixture();
        runtimeFixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        runtimeFixture.init(new AbstractModule() {}, null);

        // Create and inject dependencies into interceptor
        SpyInterceptor interceptor = ReferenceInjector.createAndInject(SpyInterceptor.class, runtimeFixture.getInjector());

        // Test interceptor logic directly
        runtimeFixture.beginTransaction();
        try {
            // Simulate preCall
            Object modifiedInput = interceptor.preCall(null, "test-input");
            assertEquals("test-input", modifiedInput);
            assertEquals(1, interceptor.getPreCallCount());

            // Simulate postCall
            Object modifiedOutput = interceptor.postCall(null, "test-input", "test-output");
            assertEquals("test-output", modifiedOutput);
            assertEquals(1, interceptor.getPostCallCount());

            runtimeFixture.commitTransaction();
        } catch (Exception e) {
            runtimeFixture.rollbackTransaction();
            throw e;
        }
    }

    /**
     * Example: Inject dependencies into existing interceptor instance.
     *
     * <p>Useful when you create the interceptor yourself and need to
     * inject its dependencies afterward.
     */
    @Test
    void testInjectIntoExistingInstance() throws Exception {
        // Initialize runtime fixture
        runtimeFixture = new JudoRuntimeFixture();
        runtimeFixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        runtimeFixture.init(new AbstractModule() {}, null);

        // Create interceptor manually
        SpyInterceptor interceptor = new SpyInterceptor();

        // Inject dependencies
        ReferenceInjector.injectReferences(interceptor, runtimeFixture.getInjector());

        // Now the interceptor has all its dependencies injected
        // and can be used for testing
        assertNotNull(interceptor);
    }

    // ==========================================
    // Example 3: Annotation-Based Interceptor Registration
    // (See AnnotationBasedInterceptorTest inner class below)
    // ==========================================

    /**
     * This inner class demonstrates using @JudoTest(interceptors = {...})
     * for declarative interceptor registration.
     *
     * <p>Note: This is shown as documentation. In real usage, this would be
     * a separate test class.
     *
     * <pre>
     * {@code
     * class MyInterceptorTest {
     *
     *     @JudoTest(
     *         modelName = "myModel",
     *         interceptors = { AuditInterceptor.class, ValidationInterceptor.class }
     *     )
     *     void testWithInterceptors(JudoRuntimeFixture fixture) {
     *         // Interceptors are automatically:
     *         // 1. Instantiated
     *         // 2. Registered with the OperationCallInterceptorProvider
     *         // 3. Have their dependencies injected
     *
     *         Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
     *
     *         // Interceptors are invoked during this call
     *         dispatcher.callOperation(...);
     *
     *         // Access interceptor provider if needed
     *         TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();
     *         assertEquals(2, provider.size());
     *     }
     * }
     * }
     * </pre>
     */
    static class AnnotationBasedInterceptorTestDocumentation {
        // This is documentation only - see the @JudoTest annotation Javadoc
        // for the actual usage pattern
    }

    // ==========================================
    // Example 4: Model-Based Integration Testing (Documentation)
    // ==========================================

    /**
     * Model-based integration tests allow you to test interceptors with actual
     * dispatcher operation calls through a real JUDO model.
     *
     * <p><strong>Prerequisites:</strong>
     * <ul>
     * <li>A generated JUDO model (ASM, RDBMS, Expression, Measure, Liquibase models)</li>
     * <li>Model files in the classpath or filesystem</li>
     * </ul>
     *
     * <p><strong>Model Generation:</strong>
     * Models are generated from JSL (JUDO Script Language) files using the
     * judo-tatami transformation pipeline. For example, the ActionGroupTest model
     * is generated from:
     * <pre>
     * /runtime/judo-tatami-jsl-tests/models/ActionGroupTest/src/main/model/ActionGroupTest.jsl
     * </pre>
     *
     * <p><strong>Example with @JudoTest annotation:</strong>
     * <pre>{@code
     * @JudoTest(
     *     modelName = "ActionGroupTest",
     *     modelSource = JudoTest.ModelSource.CLASSPATH,  // or FILESYSTEM
     *     dialect = "hsqldb",
     *     transaction = JudoTest.TransactionHandling.AUTO_ROLLBACK,
     *     interceptors = { AuditInterceptor.class }
     * )
     * void testInterceptorWithRealDispatcher(JudoRuntimeFixture fixture) {
     *     // Get the dispatcher
     *     Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
     *
     *     // Get the interceptor for verification
     *     AuditInterceptor audit = (AuditInterceptor) fixture
     *         .getInterceptorProvider().getInterceptors().get(0);
     *
     *     // Call a real operation through dispatcher
     *     // Interceptor's preCall() and postCall() will be invoked!
     *     Map<String, Object> result = dispatcher.callOperation(
     *         "ActionGroupTest.Galaxy.createInterstellarMedium",
     *         Payload.map(
     *             "__exposed", true,
     *             "matterCreator", Payload.map(
     *                 "mass", 1000.00,
     *                 "shortNote", "testmatter"
     *             )
     *         )
     *     );
     *
     *     // Verify operation completed and interceptor was invoked
     *     assertNotNull(result);
     *     assertEquals(1, audit.getCallCount());
     * }
     * }</pre>
     *
     * <p><strong>Example with programmatic setup:</strong>
     * <pre>{@code
     * void testWithModelFromDirectory() throws Exception {
     *     // Setup datasource
     *     DataSource ds = createHsqldbDatasource();
     *
     *     // Create fixture
     *     JudoRuntimeFixture fixture = new JudoRuntimeFixture();
     *
     *     // Load model from generated directory
     *     File modelDir = new File("target/generated-test-sources/model");
     *     JudoModelLoader modelLoader = JudoModelLoader.loadFromDirectory(
     *         "ActionGroupTest",
     *         modelDir,
     *         new HsqldbDialect(),
     *         false  // loadKeycloak
     *     );
     *
     *     // Prepare with loaded model
     *     fixture.prepare(modelLoader, ds, "hsqldb");
     *
     *     // Register interceptor
     *     fixture.addInterceptor(MyInterceptor.class);
     *
     *     // Initialize
     *     fixture.init(new AbstractModule() {}, null);
     *
     *     // Now use dispatcher with real operations
     *     Dispatcher dispatcher = fixture.getInjector().getInstance(Dispatcher.class);
     *     // ...
     * }
     * }</pre>
     *
     * <p><strong>Key Points:</strong>
     * <ul>
     * <li>Model files are typically NOT checked into git - they're generated during build</li>
     * <li>Use {@code JudoModelLoader.loadFromClassloader()} for packaged tests (JAR)</li>
     * <li>Use {@code JudoModelLoader.loadFromDirectory()} for development tests</li>
     * <li>The operation FQN format is: {@code modelName.TransferName.operationName}</li>
     * <li>Include {@code "__exposed": true} in the payload for public operations</li>
     * </ul>
     *
     * @see JudoModelLoader#loadFromDirectory(String, java.io.File, hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect, boolean)
     * @see JudoModelLoader#loadFromClassloader(String, ClassLoader, hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect, boolean, boolean)
     */
    static class ModelBasedIntegrationTestDocumentation {
        // This is documentation only - see Javadoc above for usage patterns
    }

    // ==========================================
    // Example 5: Testing with Transactions
    // ==========================================

    /**
     * Example: Test interceptor behavior within transactions.
     *
     * <p>Demonstrates using savepoints for partial rollback during testing.
     */
    @Test
    void testWithTransactionManagement() throws Exception {
        // Initialize runtime fixture with interceptor
        runtimeFixture = new JudoRuntimeFixture();
        runtimeFixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        runtimeFixture.addInterceptor(SpyInterceptor.class);
        runtimeFixture.init(new AbstractModule() {}, null);

        // Get the spy interceptor
        SpyInterceptor spy = (SpyInterceptor) runtimeFixture.getInterceptorProvider().getInterceptors().get(0);

        // Begin transaction
        runtimeFixture.beginTransaction();

        try {
            // Create savepoint for partial rollback
            Object savepoint = runtimeFixture.createSavePoint();

            // Execute some test logic that invokes interceptors
            // (In a real test, you'd call dispatcher operations here)
            spy.preCall(null, "test");
            assertEquals(1, spy.getPreCallCount());

            // Optionally rollback to savepoint
            runtimeFixture.rollbackToSavePoint(savepoint);

            // Continue with more test logic...

            // Commit if everything is ok
            runtimeFixture.commitTransaction();
        } catch (Exception e) {
            // Rollback on error
            runtimeFixture.rollbackTransaction();
            throw e;
        }
    }
}
