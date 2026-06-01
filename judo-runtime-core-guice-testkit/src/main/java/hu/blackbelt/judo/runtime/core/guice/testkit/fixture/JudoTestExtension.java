package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import com.google.inject.AbstractModule;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

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
public class JudoTestExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {

    private static final Logger log = LoggerFactory.getLogger(JudoTestExtension.class);

    private static final String DATASOURCE_FIXTURE_KEY = "judoDatasourceFixture";
    private static final String RUNTIME_FIXTURE_KEY = "judoRuntimeFixture";
    private static final String TRANSACTION_HANDLING_KEY = "transactionHandling";
    private static final String MODEL_LOADER_KEY = "judoModelLoader";
    private static final String CACHED_RUNTIME_KEY = "judoCachedRuntime";
    private static final String SINGLETON_RUNTIMES_HOLDER_KEY = "judoSingletonRuntimes";

    // JVM-wide map of cached datasources for SINGLETON mode, keyed by (dialect, container).
    // Replaces the previous unkeyed `singletonDatasource` field which silently shared the
    // first SINGLETON class's datasource with every subsequent SINGLETON class regardless of
    // their dialect/container configuration. See SingletonDatasourceKeyingTest.
    private static final ConcurrentHashMap<SingletonDatasourceKey, CloseableDatasourceFixture> singletonDatasources = new ConcurrentHashMap<>();

    // JVM-wide map of cached model loaders for SINGLETON mode, keyed by
    // (modelName, dialect, modelSource). Replaces the previous unkeyed `singletonModelLoader`
    // field which silently shared the first SINGLETON class's model with every subsequent
    // SINGLETON class regardless of their model configuration.
    // See SingletonModelLoaderKeyingTest.
    private static final ConcurrentHashMap<SingletonModelKey, JudoModelLoader> singletonModelLoaders = new ConcurrentHashMap<>();

    // JVM-wide map of cached runtimes for SINGLETON mode, keyed by ByClassCacheKey.
    private static final ConcurrentHashMap<ByClassCacheKey, CachedRuntime> singletonRuntimes = new ConcurrentHashMap<>();
    private static volatile boolean singletonShutdownRegistered = false;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Check if @JudoTest is on the class level
        JudoTest classAnnotation = context.getRequiredTestClass().getAnnotation(JudoTest.class);
        if (classAnnotation != null) {
            // Initialize datasource at class level for class-level annotations
            // Use BY_CLASS or SINGLETON mode as specified
            JudoTest.DataSourceMode mode = classAnnotation.dataSourceMode();
            if (mode == JudoTest.DataSourceMode.BY_CLASS || mode == JudoTest.DataSourceMode.SINGLETON) {
                initializeDatasource(context, classAnnotation);
                // Also load and cache the model at class level for BY_CLASS and SINGLETON modes
                initializeModelLoader(context, classAnnotation);
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
    private void initializeDatasource(ExtensionContext context, JudoTest annotation) throws Exception {
        ExtensionContext.Store store = getStore(context);

        // Check if already initialized
        if (store.get(DATASOURCE_FIXTURE_KEY) != null) {
            return;
        }

        JudoTest.DataSourceMode mode = annotation.dataSourceMode();
        CloseableDatasourceFixture datasourceToUse = null;

        switch (mode) {
            case SINGLETON:
                // Use or create the per-configuration singleton datasource.
                // Keyed by (dialect, container) so two SINGLETON classes with different
                // configurations do NOT silently share a datasource.
                String dsDialect = resolveDialectName(annotation);
                String dsContainer = resolveContainerName(annotation, dsDialect);
                SingletonDatasourceKey dsKey = new SingletonDatasourceKey(dsDialect, dsContainer);
                CloseableDatasourceFixture singletonForKey = singletonDatasources.get(dsKey);
                if (singletonForKey == null) {
                    synchronized (JudoTestExtension.class) {
                        singletonForKey = singletonDatasources.get(dsKey);
                        if (singletonForKey == null) {
                            JudoDatasourceFixture datasourceFixture = createDatasourceFixture(annotation);
                            singletonForKey = new CloseableDatasourceFixture(datasourceFixture);
                            singletonDatasources.put(dsKey, singletonForKey);
                            log.info("Created SINGLETON datasource for key {} (dialect: {}, container: {})", dsKey, dsDialect, dsContainer);
                            registerSingletonShutdownHook(context);
                        }
                    }
                }
                datasourceToUse = singletonForKey;
                log.debug("Using SINGLETON datasource {} for: {}", dsKey, context.getRequiredTestClass().getSimpleName());

                // Store a non-closing reference wrapper in class-scoped store so JUnit's
                // per-class teardown does not close the JVM-shared fixture.
                store.put(DATASOURCE_FIXTURE_KEY, new NonClosingDatasourceReference(singletonForKey));
                return; // Exit early - we've stored the non-closing reference
            case BY_CLASS:
            case BY_METHOD:
            default:
                // Create new datasource
                JudoDatasourceFixture datasourceFixture = createDatasourceFixture(annotation);
                datasourceToUse = new CloseableDatasourceFixture(datasourceFixture);
                log.debug("Created {} datasource for: {} (dialect: {}, container: {})", mode, context.getRequiredTestClass().getSimpleName(), annotation.dialect(), annotation.container());
                break;
        }

        store.put(DATASOURCE_FIXTURE_KEY, datasourceToUse);
    }

    /**
     * Resolves the effective dialect name from environment variable, annotation, and default.
     * Resolution priority: {@code JUDO_TEST_DIALECT} env var &gt; annotation value &gt; {@code "hsqldb"}.
     */
    private static String resolveDialectName(JudoTest annotation) {
        String dialect = System.getenv("JUDO_TEST_DIALECT");
        if (dialect == null || dialect.trim().isEmpty()) {
            dialect = annotation.dialect();
        }
        if (dialect == null || dialect.trim().isEmpty()) {
            dialect = "hsqldb";
        }
        return dialect;
    }

    /**
     * Resolves the effective container name using the same priority and auto-detection rules
     * as {@link #createDatasourceFixture(JudoTest)}: env var {@code JUDO_TEST_CONTAINER} &gt;
     * annotation value &gt; {@code "none"}, with the {@code postgresql + none} -&gt;
     * {@code postgresql} auto-detect.
     *
     * <p>Extracted for use as the {@link SingletonDatasourceKey} component so the cache key
     * exactly mirrors the values that ultimately configure the datasource.
     */
    private static String resolveContainerName(JudoTest annotation, String resolvedDialect) {
        String container = System.getenv("JUDO_TEST_CONTAINER");
        if (container == null || container.trim().isEmpty()) {
            container = annotation.container();
        }
        if (container == null || container.trim().isEmpty()) {
            container = "none";
        }
        if ("postgresql".equalsIgnoreCase(resolvedDialect) && "none".equalsIgnoreCase(container)) {
            container = "postgresql";
        }
        return container;
    }

    /**
     * Initializes and caches the model loader for BY_CLASS or SINGLETON modes.
     * This avoids reloading the model for every test method.
     */
    private void initializeModelLoader(ExtensionContext context, JudoTest annotation) throws Exception {
        ExtensionContext.Store store = getStore(context);

        // Check if already initialized
        if (store.get(MODEL_LOADER_KEY) != null) {
            return;
        }

        JudoTest.DataSourceMode mode = annotation.dataSourceMode();
        String dialect = resolveDialectName(annotation);

        switch (mode) {
            case SINGLETON:
                // Use or create the per-configuration singleton model loader.
                // Keyed by (modelName, dialect, modelSource) so two SINGLETON classes with
                // different model configurations do NOT silently share a model loader.
                SingletonModelKey modelKey = new SingletonModelKey(annotation.modelName(), dialect, annotation.modelSource());
                JudoModelLoader loaderForKey = singletonModelLoaders.get(modelKey);
                if (loaderForKey == null) {
                    synchronized (JudoTestExtension.class) {
                        loaderForKey = singletonModelLoaders.get(modelKey);
                        if (loaderForKey == null) {
                            loaderForKey = JudoRuntimeFixture.loadModel(
                                    annotation.modelName(),
                                    dialect,
                                    annotation.modelSource()
                            );
                            singletonModelLoaders.put(modelKey, loaderForKey);
                            log.info("Loaded SINGLETON model for key {}", modelKey);
                            registerSingletonShutdownHook(context);
                        }
                    }
                }
                store.put(MODEL_LOADER_KEY, loaderForKey);
                log.debug("Using SINGLETON model {} for: {}", modelKey, context.getRequiredTestClass().getSimpleName());
                break;
            case BY_CLASS:
                // Load model once per test class
                JudoModelLoader classModelLoader = JudoRuntimeFixture.loadModel(
                        annotation.modelName(),
                        dialect,
                        annotation.modelSource()
                );
                store.put(MODEL_LOADER_KEY, classModelLoader);
                log.info("Loaded BY_CLASS model '{}' for: {}", annotation.modelName(), context.getRequiredTestClass().getSimpleName());
                break;
            default:
                // BY_METHOD mode - model will be loaded per test method
                break;
        }
    }

    /**
     * Creates and configures a datasource fixture from annotation parameters.
     * Configuration priority: environment variables > annotation > defaults
     */
    private JudoDatasourceFixture createDatasourceFixture(JudoTest annotation) throws Exception {
        JudoDatasourceFixture datasourceFixture = new JudoDatasourceFixture();

        // Resolve dialect and container using the same helpers used to build the
        // SINGLETON cache key, so the cached entry exactly mirrors the values that
        // configure the underlying fixture.
        String dialect = resolveDialectName(annotation);
        String container = resolveContainerName(annotation, dialect);

        // Configure from resolved values
        datasourceFixture.setDialect(dialect);
        datasourceFixture.setContainer(container);

        log.info("Creating datasource with dialect={}, container={}", dialect, container);

        // Setup and prepare
        datasourceFixture.setupDatabase();
        datasourceFixture.prepareDatasources();

        return datasourceFixture;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        // Get annotation from method first, then fall back to class
        JudoTest annotation = context.getRequiredTestMethod().getAnnotation(JudoTest.class);

        boolean isClassLevel = false;
        if (annotation == null) {
            annotation = context.getRequiredTestClass().getAnnotation(JudoTest.class);
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

        CloseableDatasourceFixture closeableDatasource = getDatasourceFromStore(store);
        JudoDatasourceFixture datasourceFixture = closeableDatasource.datasourceFixture;

        // Create runtime fixture wrapper (always a fresh per-method instance, even on cached path).
        JudoRuntimeFixture runtimeFixture = new JudoRuntimeFixture();

        // BY_CLASS / SINGLETON + shareInjector=true: bundle the derived artifacts (QueryFactory,
        // Injector, databaseModule, Liquibase executor, transactionManager) once per scope and reuse them.
        // BY_METHOD, method-level @JudoTest, AND shareInjector=false (default): continue to call
        // prepare(...) + init(...) every method. The routing predicate is centralised in
        // JudoTestExtensionRouting for unit-testability.
        boolean useCache = JudoTestExtensionRouting.useCache(
                isClassLevel, mode, annotation.shareInjector());

        if (useCache) {
            CachedRuntime cached = getOrBuildCachedRuntime(
                    context, store, annotation, datasourceFixture);
            runtimeFixture.prepareWithCachedRuntime(cached, context.getTestInstance().orElse(null));
            log.debug("Using cached runtime for test: {} (mode={})", context.getDisplayName(), mode);
        } else {
            // Cold path — BY_METHOD or method-level annotation.
            // Only reuse a class-scoped preloaded model when the annotation was applied at
            // class level. A method-level @JudoTest may declare different model parameters
            // and MUST load its own model rather than picking up whatever the surrounding
            // class-level @JudoTest preloaded into the class-scoped store.
            JudoModelLoader cachedModelLoader = coldPathModelLoader(isClassLevel, store);
            if (cachedModelLoader != null) {
                runtimeFixture.prepareWithModel(cachedModelLoader, datasourceFixture.getDataSource(), datasourceFixture.getDialect());
                log.debug("Using cached model for test: {}", context.getDisplayName());
            } else {
                runtimeFixture.prepare(annotation.modelName(), datasourceFixture.getDataSource(), datasourceFixture.getDialect(), annotation.modelSource());
            }

            // Register interceptor classes from annotation
            Class<? extends OperationCallInterceptor>[] interceptorClasses = annotation.interceptors();
            for (Class<? extends OperationCallInterceptor> interceptorClass : interceptorClasses) {
                runtimeFixture.addInterceptor(interceptorClass);
                log.debug("Registered interceptor class from annotation: {}", interceptorClass.getName());
            }

            // Instantiate custom modules from annotation
            com.google.inject.Module customModule = createCustomModule(annotation);

            // init() will handle interceptor instantiation, registration, and dependency injection
            runtimeFixture.init(customModule, context.getTestInstance().orElse(null));
        }

        // Handle transaction based on strategy
        JudoTest.TransactionHandling transactionHandling = annotation.transaction();
        store.put(TRANSACTION_HANDLING_KEY, transactionHandling);

        if (transactionHandling == JudoTest.TransactionHandling.AUTO_ROLLBACK || transactionHandling == JudoTest.TransactionHandling.AUTO_COMMIT) {
            runtimeFixture.beginTransaction();
            log.debug("Transaction started for test: {}", context.getDisplayName());
        }

        store.put(RUNTIME_FIXTURE_KEY, runtimeFixture);

        log.info("Running test: {} (transaction: {})", context.getDisplayName(), transactionHandling);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        // Get annotation from method first, then fall back to class
        JudoTest annotation = context.getRequiredTestMethod().getAnnotation(JudoTest.class);

        boolean isClassLevel = false;
        if (annotation == null) {
            annotation = context.getRequiredTestClass().getAnnotation(JudoTest.class);
            isClassLevel = true;
        }

        if (annotation == null) {
            return;
        }

        ExtensionContext.Store store = getStore(context);
        JudoRuntimeFixture runtimeFixture = (JudoRuntimeFixture) store.get(RUNTIME_FIXTURE_KEY);
        JudoTest.TransactionHandling transactionHandling = (JudoTest.TransactionHandling) store.get(TRANSACTION_HANDLING_KEY);

        if (runtimeFixture != null && transactionHandling != null) {
            // Handle transaction cleanup
            try {
                switch (transactionHandling) {
                    case AUTO_ROLLBACK:
                        runtimeFixture.rollbackTransaction();
                        log.debug("Transaction rolled back for test: {}", context.getDisplayName());
                        break;
                    case AUTO_COMMIT:
                        runtimeFixture.commitTransaction();
                        log.debug("Transaction committed for test: {}", context.getDisplayName());

                        // Truncate tables if configured
                        if (annotation.truncateTables()) {
                            CloseableDatasourceFixture closeableDatasource = getDatasourceFromStore(store);
                            closeableDatasource.datasourceFixture.truncateTables(runtimeFixture.modelHolder.getRdbmsModel());
                            log.debug("Tables truncated after test: {}", context.getDisplayName());
                        }
                        break;
                    case MANUAL:
                        // User is responsible for transaction management
                        // But we still truncate tables if configured
                        if (annotation.truncateTables()) {
                            CloseableDatasourceFixture closeableDatasource = getDatasourceFromStore(store);
                            closeableDatasource.datasourceFixture.truncateTables(runtimeFixture.modelHolder.getRdbmsModel());
                        }
                        break;
                    case NONE:
                        // No transaction handling
                        break;
                }
            } catch (Exception e) {
                log.error("Error during transaction cleanup for test: {}", context.getDisplayName(), e);
            }

            // Always call tearDown
            try {
                runtimeFixture.tearDown();
            } catch (Exception e) {
                log.error("Error during fixture teardown for test: {}", context.getDisplayName(), e);
            }
        }

        // Clean up datasource for BY_METHOD mode
        JudoTest.DataSourceMode mode = annotation.dataSourceMode();
        if (!isClassLevel || mode == JudoTest.DataSourceMode.BY_METHOD) {
            // Remove datasource from store for BY_METHOD mode
            // This triggers cleanup via CloseableResource
            CloseableDatasourceFixture datasource = (CloseableDatasourceFixture) store.remove(DATASOURCE_FIXTURE_KEY);
            if (datasource != null && mode != JudoTest.DataSourceMode.SINGLETON) {
                // Close datasource (unless it's singleton)
                try {
                    datasource.close();
                    log.debug("Closed BY_METHOD datasource for test: {}", context.getDisplayName());
                } catch (Exception e) {
                    log.error("Error closing datasource: {}", e.getMessage(), e);
                }
            }
        }

        log.info("Completed test: {} (transaction: {}, datasource: {})", context.getDisplayName(), transactionHandling, mode);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        Class<?> parameterType = parameterContext.getParameter().getType();
        return (parameterType.equals(JudoRuntimeFixture.class) || parameterType.equals(JudoDatasourceFixture.class));
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        Class<?> parameterType = parameterContext.getParameter().getType();
        ExtensionContext.Store store = getStore(extensionContext);

        if (parameterType.equals(JudoRuntimeFixture.class)) {
            return store.get(RUNTIME_FIXTURE_KEY);
        } else if (parameterType.equals(JudoDatasourceFixture.class)) {
            CloseableDatasourceFixture closeable = getDatasourceFromStore(store);
            return closeable != null ? closeable.datasourceFixture : null;
        }

        return null;
    }

    private ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(ExtensionContext.Namespace.create(getClass(), context.getRequiredTestClass()));
    }

    /**
     * Returns the class-level store for the test class. Walks up the
     * {@link ExtensionContext} chain until a context without a test method is
     * reached (i.e. the class context). Required because {@code put()}
     * operations land at the level of the calling context, and we need values
     * written from {@code beforeEach} to survive across methods.
     */
    private ExtensionContext.Store getClassLevelStore(ExtensionContext context) {
        ExtensionContext target = context;
        while (target.getTestMethod().isPresent() && target.getParent().isPresent()) {
            target = target.getParent().get();
        }
        return target.getStore(ExtensionContext.Namespace.create(getClass(), context.getRequiredTestClass()));
    }

    /**
     * Returns the cached runtime for the current scope, building it on first call.
     * <ul>
     *   <li>{@code BY_CLASS}: stored in the class-scoped JUnit store under {@link #CACHED_RUNTIME_KEY},
     *       auto-closed by JUnit when the class store is cleaned up.</li>
     *   <li>{@code SINGLETON}: stored in a JVM-wide {@link ConcurrentHashMap} keyed by
     *       {@link ByClassCacheKey}. A single root-store {@link ExtensionContext.Store.CloseableResource}
     *       is registered exactly once to close all entries at JVM shutdown.</li>
     * </ul>
     */
    private CachedRuntime getOrBuildCachedRuntime(
            ExtensionContext context,
            ExtensionContext.Store store,
            JudoTest annotation,
            JudoDatasourceFixture datasourceFixture) {
        JudoTest.DataSourceMode mode = annotation.dataSourceMode();
        String resolvedDialect = datasourceFixture.getDialect();

        JudoModelLoader preloaded = (JudoModelLoader) store.get(MODEL_LOADER_KEY);

        if (mode == JudoTest.DataSourceMode.BY_CLASS) {
            // IMPORTANT: write the CachedRuntime to the CLASS-level store, not the
            // method-level store, otherwise it would be evicted after afterEach
            // and re-built every method — defeating the cache. Reads still work
            // from method-context because JUnit's NamespaceAwareStore walks up to
            // ancestor stores on get().
            //
            // Use {@code getOrComputeIfAbsent} (not {@code get}+{@code put}) so two
            // concurrently-executing test methods in the same class — e.g. under
            // {@code @Execution(CONCURRENT)} — cannot both build a runtime and have
            // one of them orphaned (un-closed by JUnit, leaking the HikariCP pool).
            ExtensionContext.Store classStore = getClassLevelStore(context);
            return classStore.getOrComputeIfAbsent(
                    CACHED_RUNTIME_KEY,
                    k -> buildCachedRuntime(annotation, datasourceFixture, preloaded),
                    CachedRuntime.class);
        }

        // SINGLETON: JVM-scoped map.
        ByClassCacheKey key = ByClassCacheKey.forSingleton(annotation, resolvedDialect);
        CachedRuntime cached = singletonRuntimes.get(key);
        if (cached != null) {
            return cached;
        }
        synchronized (JudoTestExtension.class) {
            cached = singletonRuntimes.get(key);
            if (cached == null) {
                cached = buildCachedRuntime(annotation, datasourceFixture, preloaded);
                singletonRuntimes.put(key, cached);
                registerSingletonShutdownHook(context);
            }
        }
        return cached;
    }

    /**
     * Builds a fresh {@link CachedRuntime} by performing the full prepare + init chain
     * once. Installs a {@link CountingLiquibaseExecutor} so subsequent test methods can
     * verify exactly-once Liquibase execution.
     */
    private CachedRuntime buildCachedRuntime(JudoTest annotation, JudoDatasourceFixture datasourceFixture, JudoModelLoader preloadedModel) {
        JudoRuntimeFixture builder = new JudoRuntimeFixture();
        // Install counting executor BEFORE prepare so initModules() picks it up.
        builder.setLiquibaseExecutor(new CountingLiquibaseExecutor());
        try {
            if (preloadedModel != null) {
                builder.prepareWithModel(preloadedModel,
                        datasourceFixture.getDataSource(),
                        datasourceFixture.getDialect());
            } else {
                builder.prepare(annotation.modelName(),
                        datasourceFixture.getDataSource(),
                        datasourceFixture.getDialect(),
                        annotation.modelSource());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare cached runtime for model '"
                    + annotation.modelName() + "'", e);
        }

        // Register interceptor classes from annotation.
        for (Class<? extends OperationCallInterceptor> interceptorClass : annotation.interceptors()) {
            builder.addInterceptor(interceptorClass);
        }

        com.google.inject.Module customModule = createCustomModule(annotation);
        builder.init(customModule, null /* member-injection deferred to per-method test instance */);

        // Eagerly resolve the PlatformTransactionManager so cached methods skip the lookup too.
        org.springframework.transaction.PlatformTransactionManager txManager =
                builder.getInjector().getInstance(org.springframework.transaction.PlatformTransactionManager.class);

        return new CachedRuntime(
                builder.modelHolder,
                builder.dialect,
                builder.queryFactory,
                builder.coercer,
                builder.databaseModule,
                builder.simpleLiquibaseExecutor,
                builder.getInjector(),
                txManager,
                // Carry the interceptor provider into the cache so cached fixtures
                // can serve getInterceptorProvider() without re-running init().
                builder.getInterceptorProvider());
    }

    /**
     * Selects the model loader to reuse on the cold path of {@code beforeEach}.
     *
     * <p>Returns the value stored under {@link #MODEL_LOADER_KEY} only when the
     * {@code @JudoTest} annotation was applied at class level. For method-level annotations
     * we MUST NOT pick up a class-scoped model loader — the method may declare different
     * {@code modelName} / {@code dialect} / {@code modelSource} and silently inheriting the
     * class-level model would yield a wrong-model fixture.
     *
     * <p>Extracted as a {@code static} helper for direct unit testing (see
     * {@code ColdPathModelLoaderTest}).
     */
    static JudoModelLoader coldPathModelLoader(boolean isClassLevel, ExtensionContext.Store store) {
        return isClassLevel ? (JudoModelLoader) store.get(MODEL_LOADER_KEY) : null;
    }

    /** Package-private accessor used by functional tests to inspect the JVM-wide SINGLETON cache. */
    static java.util.Map<ByClassCacheKey, CachedRuntime> singletonRuntimesForTesting() {
        return singletonRuntimes;
    }

    /** Package-private accessor used by functional tests to inspect the JVM-wide SINGLETON model loader map. */
    static java.util.Map<SingletonModelKey, JudoModelLoader> singletonModelLoadersForTesting() {
        return singletonModelLoaders;
    }

    /** Package-private accessor used by functional tests to inspect the JVM-wide SINGLETON datasource map. */
    static java.util.Map<SingletonDatasourceKey, CloseableDatasourceFixture> singletonDatasourcesForTesting() {
        return singletonDatasources;
    }

    /**
     * Registers a single root-store {@link ExtensionContext.Store.CloseableResource} that
     * closes every JVM-wide SINGLETON resource at the end of the test run. Idempotent: the
     * registration only happens on the first call.
     */
    private void registerSingletonShutdownHook(ExtensionContext context) {
        if (singletonShutdownRegistered) {
            return;
        }
        ExtensionContext.Store rootStore = context.getRoot().getStore(
                ExtensionContext.Namespace.create(getClass(), "singleton-runtimes"));
        rootStore.put(SINGLETON_RUNTIMES_HOLDER_KEY,
                (ExtensionContext.Store.CloseableResource) JudoTestExtension::closeAllSingletonRuntimes);
        singletonShutdownRegistered = true;
        log.debug("Registered SINGLETON shutdown hook in root store");
    }

    /**
     * Closes every SINGLETON-scoped resource exactly once at JVM shutdown:
     * {@link #singletonRuntimes}, {@link #singletonDatasources}, and clears
     * {@link #singletonModelLoaders} (model loaders themselves have no close lifecycle).
     * All three maps are cleared so a subsequent test suite within the same JVM starts fresh.
     */
    static void closeAllSingletonRuntimes() {
        for (CachedRuntime cached : singletonRuntimes.values()) {
            try {
                cached.close();
            } catch (Exception e) {
                log.error("Error closing SINGLETON cached runtime: {}", e.getMessage(), e);
            }
        }
        singletonRuntimes.clear();

        for (CloseableDatasourceFixture ds : singletonDatasources.values()) {
            try {
                ds.close();
            } catch (Exception e) {
                log.error("Error closing SINGLETON datasource: {}", e.getMessage(), e);
            }
        }
        singletonDatasources.clear();

        singletonModelLoaders.clear();
    }

    /**
     * Helper method to extract CloseableDatasourceFixture from store,
     * handling both direct CloseableDatasourceFixture and NonClosingDatasourceReference.
     */
    private CloseableDatasourceFixture getDatasourceFromStore(ExtensionContext.Store store) {
        Object stored = store.get(DATASOURCE_FIXTURE_KEY);
        if (stored == null) {
            return null;
        }
        if (stored instanceof NonClosingDatasourceReference) {
            return ((NonClosingDatasourceReference) stored).getDatasourceFixture();
        }
        return (CloseableDatasourceFixture) stored;
    }

    /**
     * Creates a combined Guice module from the custom modules specified in the annotation.
     *
     * @param annotation The @JudoTest annotation
     * @return A Guice module that combines all custom modules, or an empty module if none specified
     */
    private com.google.inject.Module createCustomModule(JudoTest annotation) {
        Class<? extends com.google.inject.Module>[] moduleClasses = annotation.modules();

        if (moduleClasses.length == 0) {
            // No custom modules - return empty module
            return new AbstractModule() {};
        }

        // Instantiate all custom modules
        java.util.List<com.google.inject.Module> modules = new java.util.ArrayList<>();
        for (Class<? extends com.google.inject.Module> moduleClass : moduleClasses) {
            try {
                com.google.inject.Module module = moduleClass.getDeclaredConstructor().newInstance();
                modules.add(module);
                log.debug("Instantiated custom Guice module: {}", moduleClass.getName());
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate custom Guice module: " + moduleClass.getName() + ". Make sure the module has a public no-arg constructor.", e);
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
     * Wrapper to make {@link JudoDatasourceFixture} closeable in JUnit's Store.
     *
     * <p>Package-private (rather than {@code private}) so functional tests in this package can
     * construct instances directly to exercise the JVM-wide SINGLETON datasource map
     * lifecycle (see {@link SingletonDatasourceKeyingTest}).
     */
    static class CloseableDatasourceFixture implements ExtensionContext.Store.CloseableResource {

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

    /**
     * Non-closing wrapper for singleton datasource references.
     * This wrapper does NOT implement CloseableResource, so JUnit will not auto-close it
     * when the class-scoped store is cleaned up. The actual singleton is registered in the
     * root store and will be closed exactly once at test-run end.
     */
    private static class NonClosingDatasourceReference {

        private final CloseableDatasourceFixture datasourceFixture;

        NonClosingDatasourceReference(CloseableDatasourceFixture datasourceFixture) {
            this.datasourceFixture = datasourceFixture;
        }

        CloseableDatasourceFixture getDatasourceFixture() {
            return datasourceFixture;
        }
    }
}
