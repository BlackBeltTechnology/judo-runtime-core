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
import hu.blackbelt.judo.runtime.core.guice.JudoDefaultModule;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb.JudoHsqldbModule;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.postgresql.JudoPostgresqlModule;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import java.io.File;
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
        SLF4JBridgeHandler.install();
        SysOutOverSLF4J.sendSystemOutAndErrToSLF4J();
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

        queryFactory = new QueryFactory(modelHolder.getAsmModel().getResourceSet(), modelHolder.getMeasureModel().getResourceSet(), asmJqlExtractor.extractExpressions(), coercer, requireNonNullElse(null, new ConcurrentHashMap<>()));
    }

    private void initModules(DataSource datasource, Dialect dialect) {
        RdbmsInit init = null;
        simpleLiquibaseExecutor = new SimpleLiquibaseExecutor();
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
        if (DIALECT_POSTGRESQL.equals(dialectName)) {
            dialect = new PostgresqlDialect();
        } else if (DIALECT_HSQLDB.equals(dialectName)) {
            dialect = new HsqldbDialect();
        } else {
            throw new IllegalArgumentException("Unsupported dialect: " + dialectName);
        }

        switch (modelSource) {
            case FILESYSTEM:
                loadModelFromFilesystem(modelName);
                break;
            case CLASSPATH:
                loadModelFromClasspath(modelName);
                break;
            case AUTO:
            default:
                // Try filesystem first, fallback to classpath
                try {
                    loadModelFromFilesystem(modelName);
                } catch (Exception e) {
                    log.warn("Failed to load model '{}' from filesystem ({}), " + "attempting to load from classpath", modelName, e.getMessage());
                    try {
                        loadModelFromClasspath(modelName);
                    } catch (Exception e2) {
                        log.error("Failed to load model '{}' from both filesystem and classpath", modelName);
                        throw new IllegalArgumentException("Could not load model '" + modelName + "'. " + "Filesystem error: " + e.getMessage() + ". " + "Classpath error: " + e2.getMessage(), e2);
                    }
                }
                break;
        }

        initQueryFactory();
        initModules(datasource, dialect);
    }

    private void loadModelFromFilesystem(String modelName) throws Exception {
        log.debug("Attempting to load model '{}' from filesystem: {}", modelName, MODEL_SOURCES);
        modelHolder = JudoModelLoader.loadFromDirectory(modelName, new File(MODEL_SOURCES), dialect, true, false);
        log.info("Successfully loaded model '{}' from filesystem", modelName);
    }

    private void loadModelFromClasspath(String modelName) throws Exception {
        log.debug("Attempting to load model '{}' from classpath", modelName);
        modelHolder = JudoModelLoader.loadFromClassloader(modelName, Thread.currentThread().getContextClassLoader(), dialect, true, false);
        log.info("Successfully loaded model '{}' from classpath", modelName);
    }

    public void init(Module module, Object injectModulesTo) {
        judoDefaultModuleBuilder = judoDefaultModuleBuilder.injectModulesTo(injectModulesTo).judoModelLoader(modelHolder).extendableCoercer(coercer).queryFactory(queryFactory);

        Module modules = Modules.combine(module, judoDefaultModuleBuilder.build(), databaseModule);
        injector = Guice.createInjector(modules);
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
}
