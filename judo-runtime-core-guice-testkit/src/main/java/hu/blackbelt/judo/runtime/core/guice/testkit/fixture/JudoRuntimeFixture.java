package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import static java.util.Objects.requireNonNullElse;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilderConfig;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbRdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.PostgresqlDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.PostgresqlRdbmsInit;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import hu.blackbelt.judo.runtime.core.guice.JudoDefaultModule;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb.JudoHsqldbModule;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.postgresql.JudoPostgresqlModule;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.ReferenceInjector;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.TestOperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.eclipse.emf.common.util.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J;

public class JudoRuntimeFixture {

    private static final Logger log = LoggerFactory.getLogger(JudoRuntimeFixture.class);

    static {
        if (Boolean.getBoolean("judo.testkit.configureLogging")) {
            SLF4JBridgeHandler.install();
            SysOutOverSLF4J.sendSystemOutAndErrToSLF4J();
        }
    }

    public JudoRuntimeFixture(Map<String, Object> context) {
        if (context == null) {
            return;
        }
    }

    public JudoRuntimeFixture() {}

    public static final String MODEL_SOURCES = "target/generated-test-sources/model";

    public static final String DIALECT_HSQLDB = "hsqldb";
    public static final String DIALECT_POSTGRESQL = "postgresql";

    Injector injector;

    private TransactionStatus transactionStatus;

    private PlatformTransactionManager transactionManager;

    public JudoModelLoader modelHolder;

    Dialect dialect;

    JudoDefaultModule.JudoDefaultModuleBuilder judoDefaultModuleBuilder;
    Module databaseModule;

    SimpleLiquibaseExecutor simpleLiquibaseExecutor;

    ExtendableCoercer coercer;

    QueryFactory queryFactory;

    // Interceptor support
    private TestOperationCallInterceptorProvider interceptorProvider;
    private final List<Class<? extends OperationCallInterceptor>> interceptorClasses = new ArrayList<>();
    private final List<OperationCallInterceptor> interceptorInstances = new ArrayList<>();

    private void initQueryFactory() {
        coercer = new DefaultCoercer();

        JqlExpressionBuilderConfig jqlExpressionBuilderConfig = new JqlExpressionBuilderConfig();
        jqlExpressionBuilderConfig.setResolveOnlyCurrentLambdaScope(false);

        final AsmJqlExtractor asmJqlExtractor = new AsmJqlExtractor(
            modelHolder.getAsmModel().getResourceSet(),
            modelHolder.getMeasureModel().getResourceSet(),
            URI.createURI("expr:" + modelHolder.getAsmModel().getName()),
            jqlExpressionBuilderConfig
        );

        queryFactory = new QueryFactory(modelHolder.getAsmModel().getResourceSet(), modelHolder.getMeasureModel().getResourceSet(), asmJqlExtractor.extractExpressions(), coercer, new ConcurrentHashMap<>());
    }

    private void initModules(DataSource datasource, Dialect dialect) {
        RdbmsInit init = null;
        // Allow externally-supplied executor (e.g. CountingLiquibaseExecutor) — only create
        // a fresh one when no caller has pre-set it. Required by the BY_CLASS / SINGLETON
        // runtime cache so the cold path can install a counting wrapper for regression tests.
        if (simpleLiquibaseExecutor == null) {
            simpleLiquibaseExecutor = new SimpleLiquibaseExecutor();
        }
        if (dialect instanceof HsqldbDialect) {
            init = HsqldbRdbmsInit.builder().liquibaseExecutor(simpleLiquibaseExecutor).liquibaseModel(modelHolder.getLiquibaseModel()).build();
            databaseModule = JudoHsqldbModule.builder().dataSource(datasource).build();
        }
        if (dialect instanceof PostgresqlDialect) {
            init = PostgresqlRdbmsInit.builder().liquibaseExecutor(simpleLiquibaseExecutor).liquibaseModel(modelHolder.getLiquibaseModel()).build();
            databaseModule = JudoPostgresqlModule.builder().dataSource(datasource).build();
        }
        init.execute(datasource);

        judoDefaultModuleBuilder = JudoDefaultModule.builder().judoModelLoader(modelHolder);
    }

    public void prepare(String modelName, DataSource datasource, String dialectName) throws Exception {
        prepare(modelName, datasource, dialectName, JudoTest.ModelSource.AUTO);
    }

    public void prepare(String modelName, DataSource datasource, String dialectName, JudoTest.ModelSource modelSource) throws Exception {
        dialect = resolveDialect(dialectName);
        modelHolder = loadModel(modelName, dialectName, modelSource);
        initQueryFactory();
        initModules(datasource, dialect);
    }

    /**
     * Fast path used by {@link JudoTestExtension} when a {@link CachedRuntime} is
     * available for the current scope ({@code BY_CLASS} or {@code SINGLETON}).
     * Installs the cached fields and runs Guice member-injection on the test instance,
     * bypassing {@code initQueryFactory}, {@code initModules}, and {@code Guice.createInjector}.
     *
     * @param cached the cached runtime bundle (must not be null)
     * @param injectModulesTo optional test instance to receive {@code injector.injectMembers(...)};
     *                        if null, no injection is performed.
     */
    void prepareWithCachedRuntime(CachedRuntime cached, Object injectModulesTo) {
        if (cached == null) {
            throw new IllegalArgumentException("cached runtime must not be null");
        }
        this.modelHolder = cached.modelLoader;
        this.dialect = cached.dialect;
        this.queryFactory = cached.queryFactory;
        this.coercer = cached.coercer;
        this.databaseModule = cached.databaseModule;
        this.simpleLiquibaseExecutor = cached.liquibaseExecutor;
        this.injector = cached.injector;
        this.transactionManager = cached.transactionManager;
        if (injectModulesTo != null) {
            cached.injector.injectMembers(injectModulesTo);
        }
    }

    /** Package-private accessor used by regression tests. */
    QueryFactory queryFactory() {
        return queryFactory;
    }

    /** Package-private accessor used by regression tests. */
    PlatformTransactionManager transactionManager() {
        return transactionManager;
    }

    /**
     * Pre-installs an externally-built Liquibase executor (typically a counting
     * wrapper) before {@link #prepare} runs. Must be called before {@code prepare}
     * for the override to take effect.
     *
     * <p>Package-private — internal testkit seam.
     */
    void setLiquibaseExecutor(SimpleLiquibaseExecutor executor) {
        this.simpleLiquibaseExecutor = executor;
    }

    /**
     * Prepares the runtime fixture using a pre-loaded model.
     * This is useful when running in BY_CLASS or SINGLETON mode to avoid reloading the model for each test method.
     *
     * @param preloadedModel The pre-loaded JudoModelLoader instance
     * @param datasource The datasource to use
     * @param dialectName The dialect name ("hsqldb" or "postgresql")
     * @throws Exception if preparation fails
     */
    public void prepareWithModel(JudoModelLoader preloadedModel, DataSource datasource, String dialectName) throws Exception {
        dialect = resolveDialect(dialectName);
        this.modelHolder = preloadedModel;
        log.debug("Using pre-loaded model: {}", preloadedModel.getAsmModel().getName());

        initQueryFactory();
        initModules(datasource, dialect);
    }

    /**
     * Loads the model based on the specified source.
     * This is a public method to allow loading the model separately for caching purposes.
     *
     * @param modelName The name of the model to load
     * @param dialectName The dialect name ("hsqldb" or "postgresql")
     * @param modelSource The source from which to load the model
     * @return The loaded JudoModelLoader instance
     * @throws Exception if model loading fails
     */
    /**
     * Resolves a dialect name string to a {@link Dialect} instance.
     *
     * @param dialectName The dialect name ("hsqldb" or "postgresql")
     * @return the corresponding Dialect instance
     * @throws IllegalArgumentException if the dialect name is not recognised
     */
    static Dialect resolveDialect(String dialectName) {
        if (DIALECT_POSTGRESQL.equals(dialectName)) {
            return new PostgresqlDialect();
        } else if (DIALECT_HSQLDB.equals(dialectName)) {
            return new HsqldbDialect();
        }
        throw new IllegalArgumentException("Unsupported dialect: " + dialectName);
    }

    /**
     * Loads the model based on the specified source.
     * This is a public method to allow loading the model separately for caching purposes.
     *
     * @param modelName The name of the model to load
     * @param dialectName The dialect name ("hsqldb" or "postgresql")
     * @param modelSource The source from which to load the model
     * @return The loaded JudoModelLoader instance
     * @throws Exception if model loading fails
     */
    public static JudoModelLoader loadModel(String modelName, String dialectName, JudoTest.ModelSource modelSource) throws Exception {
        Dialect loadDialect = resolveDialect(dialectName);

        switch (modelSource) {
            case FILESYSTEM:
                return loadModelFromFilesystem(modelName, loadDialect);
            case CLASSPATH:
                return loadModelFromClasspath(modelName, loadDialect);
            case AUTO:
            default:
                return loadModelAuto(modelName, loadDialect);
        }
    }

    private static JudoModelLoader loadModelFromFilesystem(String modelName, Dialect loadDialect) throws Exception {
        log.debug("Loading model '{}' from filesystem: {}", modelName, MODEL_SOURCES);
        JudoModelLoader model = JudoModelLoader.loadFromDirectory(modelName, new File(MODEL_SOURCES), loadDialect, true, false);
        log.info("Successfully loaded model '{}' from filesystem", modelName);
        return model;
    }

    private static JudoModelLoader loadModelFromClasspath(String modelName, Dialect loadDialect) throws Exception {
        log.debug("Loading model '{}' from classpath", modelName);
        JudoModelLoader model = JudoModelLoader.loadFromClassloader(modelName, Thread.currentThread().getContextClassLoader(), loadDialect, true, false);
        log.info("Successfully loaded model '{}' from classpath", modelName);
        return model;
    }

    private static JudoModelLoader loadModelAuto(String modelName, Dialect loadDialect) throws Exception {
        try {
            return loadModelFromFilesystem(modelName, loadDialect);
        } catch (Exception e) {
            log.warn("Failed to load model '{}' from filesystem ({}), attempting to load from classpath", modelName, e.getMessage());
            try {
                return loadModelFromClasspath(modelName, loadDialect);
            } catch (Exception e2) {
                log.error("Failed to load model '{}' from both filesystem and classpath", modelName);
                throw new IllegalArgumentException("Could not load model '" + modelName + "'. "
                        + "Filesystem error: " + e.getMessage() + ". "
                        + "Classpath error: " + e2.getMessage(), e2);
            }
        }
    }

    public void init(Module module, Object injectModulesTo) {
        // 1. Create interceptor provider
        interceptorProvider = new TestOperationCallInterceptorProvider();

        // 2. Instantiate interceptor classes
        for (Class<? extends OperationCallInterceptor> clazz : interceptorClasses) {
            try {
                OperationCallInterceptor instance = clazz.getDeclaredConstructor().newInstance();
                interceptorInstances.add(instance);
                log.debug("Instantiated interceptor: {}", clazz.getName());
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Interceptor class " + clazz.getName() + " must have a public no-arg constructor", e);
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate interceptor: " + clazz.getName(), e);
            }
        }

        // 3. Register all instances in provider
        for (OperationCallInterceptor interceptor : interceptorInstances) {
            interceptorProvider.addInterceptor(interceptor);
        }

        // 4. Build module with custom provider
        judoDefaultModuleBuilder = judoDefaultModuleBuilder.injectModulesTo(injectModulesTo).judoModelLoader(modelHolder).extendableCoercer(coercer).queryFactory(queryFactory).operationCallInterceptorProvider(interceptorProvider);

        Module modules = Modules.combine(module, judoDefaultModuleBuilder.build(), databaseModule);

        // 5. Create injector
        injector = Guice.createInjector(modules);

        // 6. DEFERRED INJECTION - inject dependencies into interceptors
        for (OperationCallInterceptor interceptor : interceptorInstances) {
            ReferenceInjector.injectReferences(interceptor, injector);
            log.debug("Injected dependencies into interceptor: {}", interceptor.getClass().getName());
        }
    }

    public void tearDown() {
        // Only rollback if a transaction exists and is not completed
        if (transactionStatus != null && !transactionStatus.isCompleted()) {
            rollbackTransaction();
        }
    }

    private PlatformTransactionManager getTransactionManager() {
        if (transactionManager == null) {
            transactionManager = injector.getInstance(PlatformTransactionManager.class);
        }
        return transactionManager;
    }

    public void beginTransaction() {
        if (transactionStatus != null && !transactionStatus.isCompleted()) {
            throw new IllegalStateException("Previous transaction was not completed");
        }
        transactionStatus = getTransactionManager().getTransaction(new DefaultTransactionDefinition());
    }

    public void commitTransaction() {
        checkTransactionStatus();
        getTransactionManager().commit(transactionStatus);
    }

    public void rollbackTransaction() {
        checkTransactionStatus();
        getTransactionManager().rollback(transactionStatus);
    }

    public Object createSavePoint() {
        checkTransactionStatus();
        return transactionStatus.createSavepoint();
    }

    public void rollbackToSavePoint(Object savePoint) {
        checkTransactionStatus();
        transactionStatus.rollbackToSavepoint(savePoint);
    }

    private void checkTransactionStatus() {
        if (transactionStatus == null) {
            throw new IllegalStateException("TransactionStatus is null");
        }
        if (transactionStatus.isCompleted()) {
            throw new IllegalStateException("Transaction was already completed");
        }
    }

    /**
     * Returns the Guice injector for advanced use cases.
     * Useful for injecting @Reference dependencies into custom implementations during testing.
     *
     * @return The Guice injector instance
     * @throws IllegalStateException if injector has not been initialized (call init() first)
     */
    public Injector getInjector() {
        if (injector == null) {
            throw new IllegalStateException("Injector has not been initialized. Call init() first.");
        }
        return injector;
    }

    /**
     * Register an interceptor class to be instantiated and injected automatically.
     * Must be called before {@link #init(Module, Object)}.
     *
     * <p>The interceptor class must have a public no-arg constructor. Dependencies annotated
     * with @Reference will be automatically injected after the Guice injector is created.
     *
     * <p>Example:
     * <pre>
     * {@code
     * fixture.addInterceptor(MyInterceptor.class);
     * fixture.init(module, null);
     * // MyInterceptor is now registered and will be invoked during dispatcher calls
     * }
     * </pre>
     *
     * @param interceptorClass The interceptor class (must have no-arg constructor)
     * @throws IllegalStateException if called after init()
     */
    public void addInterceptor(Class<? extends OperationCallInterceptor> interceptorClass) {
        if (injector != null) {
            throw new IllegalStateException("Cannot add interceptor after init() has been called. " + "Use getInterceptorProvider().addInterceptor() for runtime modifications.");
        }
        if (interceptorClass == null) {
            throw new IllegalArgumentException("Interceptor class cannot be null");
        }
        this.interceptorClasses.add(interceptorClass);
        log.debug("Registered interceptor class: {}", interceptorClass.getName());
    }

    /**
     * Register a pre-created interceptor instance.
     * Dependencies will be injected after {@link #init(Module, Object)} is called.
     * Must be called before init().
     *
     * <p>Use this method when your interceptor requires constructor parameters or
     * custom configuration before injection.
     *
     * <p>Example:
     * <pre>
     * {@code
     * MyInterceptor interceptor = new MyInterceptor("custom-config");
     * fixture.addInterceptor(interceptor);
     * fixture.init(module, null);
     * // interceptor now has its @Reference dependencies injected
     * }
     * </pre>
     *
     * @param interceptor The interceptor instance
     * @throws IllegalStateException if called after init()
     */
    public void addInterceptor(OperationCallInterceptor interceptor) {
        if (injector != null) {
            throw new IllegalStateException("Cannot add interceptor after init() has been called. " + "Use getInterceptorProvider().addInterceptor() for runtime modifications.");
        }
        if (interceptor == null) {
            throw new IllegalArgumentException("Interceptor cannot be null");
        }
        this.interceptorInstances.add(interceptor);
        log.debug("Registered interceptor instance: {}", interceptor.getClass().getName());
    }

    /**
     * Get the interceptor provider for advanced manipulation.
     * Available after {@link #init(Module, Object)} is called.
     *
     * <p>Use this to add, remove, or clear interceptors at runtime during a test.
     *
     * <p>Example:
     * <pre>
     * {@code
     * // Clear all interceptors mid-test
     * fixture.getInterceptorProvider().clearInterceptors();
     *
     * // Add a new interceptor at runtime (note: dependencies won't be auto-injected)
     * fixture.getInterceptorProvider().addInterceptor(newInterceptor);
     * }
     * </pre>
     *
     * @return The test interceptor provider
     * @throws IllegalStateException if called before init()
     */
    public TestOperationCallInterceptorProvider getInterceptorProvider() {
        if (interceptorProvider == null) {
            throw new IllegalStateException("Interceptor provider not available. Call init() first.");
        }
        return interceptorProvider;
    }
}
