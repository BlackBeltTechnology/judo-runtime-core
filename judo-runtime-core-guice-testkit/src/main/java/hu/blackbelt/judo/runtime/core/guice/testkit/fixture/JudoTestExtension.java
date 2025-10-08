package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import com.google.inject.AbstractModule;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit Jupiter extension that provides JUDO runtime fixture setup for tests annotated with @JudoTest.
 *
 * <p>This extension integrates with the JUDO testkit fixture infrastructure and provides:
 * <ul>
 * <li>Automatic datasource setup (HSQLDB or PostgreSQL)</li>
 * <li>JUDO runtime initialization with model loading</li>
 * <li>Configurable transaction management (auto-rollback, auto-commit, manual, none)</li>
 * <li>Optional table truncation after tests</li>
 * <li>JudoRuntimeFixture parameter injection</li>
 * </ul>
 *
 * <p>The extension uses JudoDatasourceByClassExtension and JudoRuntimeFixture internally,
 * following the same patterns as other fixture extensions.
 *
 * @see JudoTest
 * @see JudoRuntimeExtension
 * @see JudoDatasourceByClassExtension
 */
public class JudoTestExtension
    implements
        BeforeAllCallback,
        AfterAllCallback,
        BeforeEachCallback,
        AfterEachCallback,
        ParameterResolver {

    private static final Logger log = LoggerFactory.getLogger(
        JudoTestExtension.class
    );

    private static final String DATASOURCE_FIXTURE_KEY =
        "judoDatasourceFixture";
    private static final String RUNTIME_FIXTURE_KEY = "judoRuntimeFixture";
    private static final String TRANSACTION_HANDLING_KEY =
        "transactionHandling";
    private static final String SINGLETON_DATASOURCE_KEY =
        "singletonDatasource";

    // Singleton datasource shared across all tests (for SINGLETON mode)
    private static volatile CloseableDatasourceFixture singletonDatasource;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Check if @JudoTest is on the class level
        JudoTest classAnnotation = context
            .getRequiredTestClass()
            .getAnnotation(JudoTest.class);
        if (classAnnotation != null) {
            // Initialize datasource at class level for class-level annotations
            // Use BY_CLASS or SINGLETON mode as specified
            JudoTest.DataSourceMode mode = classAnnotation.dataSourceMode();
            if (
                mode == JudoTest.DataSourceMode.BY_CLASS ||
                mode == JudoTest.DataSourceMode.SINGLETON
            ) {
                initializeDatasource(context, classAnnotation);
            }
            // For BY_METHOD mode with class-level annotation,
            // initialization happens in beforeEach()
        }
        // For method-level annotations, initialization happens in beforeEach()
    }

    @Override
    public void afterAll(ExtensionContext context) {
        // Cleanup handled by CloseableResource
    }

    /**
     * Initializes the datasource fixture based on the datasource mode.
     * Can be called from beforeAll (for class-level @JudoTest) or beforeEach (for method-level @JudoTest).
     */
    private void initializeDatasource(
        ExtensionContext context,
        JudoTest annotation
    ) throws Exception {
        ExtensionContext.Store store = getStore(context);

        // Check if already initialized
        if (store.get(DATASOURCE_FIXTURE_KEY) != null) {
            return;
        }

        JudoTest.DataSourceMode mode = annotation.dataSourceMode();
        CloseableDatasourceFixture datasourceToUse = null;

        switch (mode) {
            case SINGLETON:
                // Use or create singleton datasource
                if (singletonDatasource == null) {
                    synchronized (JudoTestExtension.class) {
                        if (singletonDatasource == null) {
                            JudoDatasourceFixture datasourceFixture =
                                createDatasourceFixture(annotation);
                            singletonDatasource =
                                new CloseableDatasourceFixture(
                                    datasourceFixture
                                );
                            log.info(
                                "Created SINGLETON datasource shared across all test classes (dialect: {}, container: {})",
                                annotation.dialect(),
                                annotation.container()
                            );
                        }
                    }
                }
                datasourceToUse = singletonDatasource;
                log.debug(
                    "Using SINGLETON datasource for: {}",
                    context.getRequiredTestClass().getSimpleName()
                );
                break;
            case BY_CLASS:
            case BY_METHOD:
            default:
                // Create new datasource
                JudoDatasourceFixture datasourceFixture =
                    createDatasourceFixture(annotation);
                datasourceToUse = new CloseableDatasourceFixture(
                    datasourceFixture
                );
                log.debug(
                    "Created {} datasource for: {} (dialect: {}, container: {})",
                    mode,
                    context.getRequiredTestClass().getSimpleName(),
                    annotation.dialect(),
                    annotation.container()
                );
                break;
        }

        store.put(DATASOURCE_FIXTURE_KEY, datasourceToUse);
    }

    /**
     * Creates and configures a datasource fixture from annotation parameters.
     * Configuration priority: environment variables > annotation > defaults
     */
    private JudoDatasourceFixture createDatasourceFixture(JudoTest annotation)
        throws Exception {
        JudoDatasourceFixture datasourceFixture = new JudoDatasourceFixture();

        // Resolve dialect: env var > annotation > default (hsqldb)
        String dialect = System.getenv("JUDO_TEST_DIALECT");
        if (dialect == null || dialect.trim().isEmpty()) {
            dialect = annotation.dialect();
        }
        if (dialect == null || dialect.trim().isEmpty()) {
            dialect = "hsqldb";
        }

        // Resolve container: env var > annotation > auto-detect
        String container = System.getenv("JUDO_TEST_CONTAINER");
        if (container == null || container.trim().isEmpty()) {
            container = annotation.container();
        }
        if (container == null || container.trim().isEmpty()) {
            container = "none";
        }

        // Auto-detect: if dialect is postgresql and container is "none", use postgresql container
        if (
            "postgresql".equalsIgnoreCase(dialect) &&
            "none".equalsIgnoreCase(container)
        ) {
            container = "postgresql";
            log.debug(
                "Auto-detected container=postgresql for dialect=postgresql"
            );
        }

        // Configure from resolved values
        datasourceFixture.setDialect(dialect);
        datasourceFixture.setContainer(container);

        log.info(
            "Creating datasource with dialect={}, container={}",
            dialect,
            container
        );

        // Setup and prepare
        datasourceFixture.setupDatabase();
        datasourceFixture.prepareDatasources();

        return datasourceFixture;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        // Get annotation from method first, then fall back to class
        JudoTest annotation = context
            .getRequiredTestMethod()
            .getAnnotation(JudoTest.class);

        boolean isClassLevel = false;
        if (annotation == null) {
            annotation = context
                .getRequiredTestClass()
                .getAnnotation(JudoTest.class);
            isClassLevel = true;
        }

        if (annotation == null) {
            return;
        }

        ExtensionContext.Store store = getStore(context);

        // Determine datasource mode
        JudoTest.DataSourceMode mode = annotation.dataSourceMode();

        // Initialize datasource if not already done
        // For method-level annotations: always BY_METHOD (ignore mode setting)
        // For class-level annotations: use the specified mode
        if (!isClassLevel) {
            // Method-level annotation always uses BY_METHOD
            initializeDatasource(context, annotation);
        } else {
            // Class-level annotation with BY_METHOD mode
            if (mode == JudoTest.DataSourceMode.BY_METHOD) {
                initializeDatasource(context, annotation);
            }
            // BY_CLASS and SINGLETON already initialized in beforeAll
        }

        CloseableDatasourceFixture closeableDatasource =
            (CloseableDatasourceFixture) store.get(DATASOURCE_FIXTURE_KEY);
        JudoDatasourceFixture datasourceFixture =
            closeableDatasource.datasourceFixture;

        // Create and initialize runtime fixture
        JudoRuntimeFixture runtimeFixture = new JudoRuntimeFixture();
        runtimeFixture.prepare(
            annotation.modelName(),
            datasourceFixture.getDataSource(),
            datasourceFixture.getDialect(),
            annotation.modelSource()
        );

        // Instantiate custom modules from annotation
        com.google.inject.Module customModule = createCustomModule(annotation);

        runtimeFixture.init(
            customModule,
            context.getTestInstance().orElse(null)
        );

        // Handle transaction based on strategy
        JudoTest.TransactionHandling transactionHandling =
            annotation.transaction();
        store.put(TRANSACTION_HANDLING_KEY, transactionHandling);

        if (
            transactionHandling == JudoTest.TransactionHandling.AUTO_ROLLBACK ||
            transactionHandling == JudoTest.TransactionHandling.AUTO_COMMIT
        ) {
            runtimeFixture.beginTransaction();
            log.debug(
                "Transaction started for test: {}",
                context.getDisplayName()
            );
        }

        store.put(RUNTIME_FIXTURE_KEY, runtimeFixture);

        log.info(
            "Running test: {} (transaction: {})",
            context.getDisplayName(),
            transactionHandling
        );
    }

    @Override
    public void afterEach(ExtensionContext context) {
        // Get annotation from method first, then fall back to class
        JudoTest annotation = context
            .getRequiredTestMethod()
            .getAnnotation(JudoTest.class);

        boolean isClassLevel = false;
        if (annotation == null) {
            annotation = context
                .getRequiredTestClass()
                .getAnnotation(JudoTest.class);
            isClassLevel = true;
        }

        if (annotation == null) {
            return;
        }

        ExtensionContext.Store store = getStore(context);
        JudoRuntimeFixture runtimeFixture = (JudoRuntimeFixture) store.get(
            RUNTIME_FIXTURE_KEY
        );
        JudoTest.TransactionHandling transactionHandling =
            (JudoTest.TransactionHandling) store.get(TRANSACTION_HANDLING_KEY);

        if (runtimeFixture != null && transactionHandling != null) {
            // Handle transaction cleanup
            try {
                switch (transactionHandling) {
                    case AUTO_ROLLBACK:
                        runtimeFixture.rollbackTransaction();
                        log.debug(
                            "Transaction rolled back for test: {}",
                            context.getDisplayName()
                        );
                        break;
                    case AUTO_COMMIT:
                        runtimeFixture.commitTransaction();
                        log.debug(
                            "Transaction committed for test: {}",
                            context.getDisplayName()
                        );

                        // Truncate tables if configured
                        if (annotation.truncateTables()) {
                            CloseableDatasourceFixture closeableDatasource =
                                (CloseableDatasourceFixture) store.get(
                                    DATASOURCE_FIXTURE_KEY
                                );
                            closeableDatasource.datasourceFixture.truncateTables(
                                runtimeFixture.modelHolder.getRdbmsModel()
                            );
                            log.debug(
                                "Tables truncated after test: {}",
                                context.getDisplayName()
                            );
                        }
                        break;
                    case MANUAL:
                        // User is responsible for transaction management
                        // But we still truncate tables if configured
                        if (annotation.truncateTables()) {
                            CloseableDatasourceFixture closeableDatasource =
                                (CloseableDatasourceFixture) store.get(
                                    DATASOURCE_FIXTURE_KEY
                                );
                            closeableDatasource.datasourceFixture.truncateTables(
                                runtimeFixture.modelHolder.getRdbmsModel()
                            );
                        }
                        break;
                    case NONE:
                        // No transaction handling
                        break;
                }
            } catch (Exception e) {
                log.error(
                    "Error during transaction cleanup for test: {}",
                    context.getDisplayName(),
                    e
                );
            }

            // Always call tearDown
            try {
                runtimeFixture.tearDown();
            } catch (Exception e) {
                log.error(
                    "Error during fixture teardown for test: {}",
                    context.getDisplayName(),
                    e
                );
            }
        }

        // Clean up datasource for BY_METHOD mode
        JudoTest.DataSourceMode mode = annotation.dataSourceMode();
        if (!isClassLevel || mode == JudoTest.DataSourceMode.BY_METHOD) {
            // Remove datasource from store for BY_METHOD mode
            // This triggers cleanup via CloseableResource
            CloseableDatasourceFixture datasource =
                (CloseableDatasourceFixture) store.remove(
                    DATASOURCE_FIXTURE_KEY
                );
            if (
                datasource != null && mode != JudoTest.DataSourceMode.SINGLETON
            ) {
                // Close datasource (unless it's singleton)
                try {
                    datasource.close();
                    log.debug(
                        "Closed BY_METHOD datasource for test: {}",
                        context.getDisplayName()
                    );
                } catch (Exception e) {
                    log.error(
                        "Error closing datasource: {}",
                        e.getMessage(),
                        e
                    );
                }
            }
        }

        log.info(
            "Completed test: {} (transaction: {}, datasource: {})",
            context.getDisplayName(),
            transactionHandling,
            mode
        );
    }

    @Override
    public boolean supportsParameter(
        ParameterContext parameterContext,
        ExtensionContext extensionContext
    ) {
        Class<?> parameterType = parameterContext.getParameter().getType();
        return (
            parameterType.equals(JudoRuntimeFixture.class) ||
            parameterType.equals(JudoDatasourceFixture.class)
        );
    }

    @Override
    public Object resolveParameter(
        ParameterContext parameterContext,
        ExtensionContext extensionContext
    ) {
        Class<?> parameterType = parameterContext.getParameter().getType();
        ExtensionContext.Store store = getStore(extensionContext);

        if (parameterType.equals(JudoRuntimeFixture.class)) {
            return store.get(RUNTIME_FIXTURE_KEY);
        } else if (parameterType.equals(JudoDatasourceFixture.class)) {
            CloseableDatasourceFixture closeable =
                (CloseableDatasourceFixture) store.get(DATASOURCE_FIXTURE_KEY);
            return closeable != null ? closeable.datasourceFixture : null;
        }

        return null;
    }

    private ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(
            ExtensionContext.Namespace.create(
                getClass(),
                context.getRequiredTestClass()
            )
        );
    }

    /**
     * Creates a combined Guice module from the custom modules specified in the annotation.
     *
     * @param annotation The @JudoTest annotation
     * @return A Guice module that combines all custom modules, or an empty module if none specified
     */
    private com.google.inject.Module createCustomModule(JudoTest annotation) {
        Class<? extends com.google.inject.Module>[] moduleClasses =
            annotation.modules();

        if (moduleClasses.length == 0) {
            // No custom modules - return empty module
            return new AbstractModule() {};
        }

        // Instantiate all custom modules
        java.util.List<com.google.inject.Module> modules =
            new java.util.ArrayList<>();
        for (Class<
            ? extends com.google.inject.Module
        > moduleClass : moduleClasses) {
            try {
                com.google.inject.Module module = moduleClass
                    .getDeclaredConstructor()
                    .newInstance();
                modules.add(module);
                log.debug(
                    "Instantiated custom Guice module: {}",
                    moduleClass.getName()
                );
            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to instantiate custom Guice module: " +
                        moduleClass.getName() +
                        ". Make sure the module has a public no-arg constructor.",
                    e
                );
            }
        }

        // Combine all modules using Guice's Modules.combine
        if (modules.size() == 1) {
            return modules.get(0);
        } else {
            return com.google.inject.util.Modules.combine(modules);
        }
    }

    /**
     * Wrapper to make JudoDatasourceFixture closeable in JUnit's Store
     */
    private static class CloseableDatasourceFixture
        implements ExtensionContext.Store.CloseableResource {

        private final JudoDatasourceFixture datasourceFixture;

        CloseableDatasourceFixture(JudoDatasourceFixture datasourceFixture) {
            this.datasourceFixture = datasourceFixture;
        }

        @Override
        public void close() {
            if (datasourceFixture != null) {
                datasourceFixture.teardownDatasource();
            }
        }
    }
}
