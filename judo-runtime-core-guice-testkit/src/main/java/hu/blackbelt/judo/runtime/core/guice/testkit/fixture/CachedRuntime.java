package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import com.google.inject.Injector;
import com.google.inject.Module;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bundle of derived runtime artifacts cached at the test-class scope
 * (for {@code BY_CLASS}) or in a JVM-wide map (for {@code SINGLETON}).
 *
 * <p>Implements {@link ExtensionContext.Store.CloseableResource} so JUnit
 * automatically closes the cached injector when the owning store is cleaned up.
 *
 * <p>{@link #close()} is idempotent: a {@code closed} flag prevents
 * double-close on re-entry from JUnit or the SINGLETON shutdown hook.
 *
 * <p>Package-private intentionally: an internal testkit seam.
 */
final class CachedRuntime implements ExtensionContext.Store.CloseableResource {

    private static final Logger log = LoggerFactory.getLogger(CachedRuntime.class);

    final JudoModelLoader modelLoader;
    final Dialect dialect;
    final QueryFactory queryFactory;
    final ExtendableCoercer coercer;
    final Module databaseModule;
    final SimpleLiquibaseExecutor liquibaseExecutor;
    final Injector injector;
    final PlatformTransactionManager transactionManager;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    CachedRuntime(
            JudoModelLoader modelLoader,
            Dialect dialect,
            QueryFactory queryFactory,
            ExtendableCoercer coercer,
            Module databaseModule,
            SimpleLiquibaseExecutor liquibaseExecutor,
            Injector injector,
            PlatformTransactionManager transactionManager) {
        this.modelLoader = modelLoader;
        this.dialect = dialect;
        this.queryFactory = queryFactory;
        this.coercer = coercer;
        this.databaseModule = databaseModule;
        this.liquibaseExecutor = liquibaseExecutor;
        this.injector = injector;
        this.transactionManager = transactionManager;
    }

    /** @return whether {@link #close()} has been invoked at least once. */
    boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return; // idempotent: silently ignore second close
        }
        // Best-effort: dispose any singleton CloseableResource bindings (e.g. HikariCP pool).
        // Guice does not expose a generic injector.close(); we rely on the underlying
        // datasource being closed by JudoDatasourceFixture#teardownDatasource at the
        // appropriate scope (BY_CLASS / SINGLETON). Here we only mark closed so the
        // idempotency invariant holds.
        log.debug("Closed cached runtime (model={})",
                modelLoader != null ? modelLoader.getAsmModel().getName() : "<null>");
    }
}
