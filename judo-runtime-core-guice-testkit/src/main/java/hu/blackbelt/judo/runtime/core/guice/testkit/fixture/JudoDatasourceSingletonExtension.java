package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.extension.*;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

public class JudoDatasourceSingletonExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {

    private static final JudoDatasourceFixture JUDO_DATASOURCE_FIXTURE = new JudoDatasourceFixture();
    private static boolean initialized = false;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!initialized) {
            JUDO_DATASOURCE_FIXTURE.setupDatabase();
            JUDO_DATASOURCE_FIXTURE.prepareDatasources();
            initialized = true;
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        // rdbmsDatasourceFixture.teardownDatasource();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().isAssignableFrom(JudoDatasourceFixture.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return JUDO_DATASOURCE_FIXTURE;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        try {
            TransactionStatus transactionStatus = JUDO_DATASOURCE_FIXTURE.getTransactionManager().getTransaction(new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_MANDATORY));
            JUDO_DATASOURCE_FIXTURE.getTransactionManager().commit(transactionStatus);
        } catch (IllegalTransactionStateException ignored) {
            // Expected when no transaction is active - ignore
        }
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {}
}
