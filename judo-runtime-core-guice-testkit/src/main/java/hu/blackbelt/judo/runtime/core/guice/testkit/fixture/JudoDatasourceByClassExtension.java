package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.extension.*;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

public class JudoDatasourceByClassExtension
    implements
        BeforeAllCallback,
        AfterAllCallback,
        AfterEachCallback,
        ParameterResolver {

    private JudoDatasourceFixture judoDatasourceFixture =
        new JudoDatasourceFixture();

    @Override
    public void beforeAll(ExtensionContext context) {
        judoDatasourceFixture.setupDatabase();
        judoDatasourceFixture.prepareDatasources();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        judoDatasourceFixture.teardownDatasource();
    }

    @Override
    public boolean supportsParameter(
        ParameterContext parameterContext,
        ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        return parameterContext
            .getParameter()
            .getType()
            .isAssignableFrom(JudoDatasourceFixture.class);
    }

    @Override
    public Object resolveParameter(
        ParameterContext parameterContext,
        ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        return judoDatasourceFixture;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        try {
            TransactionStatus transactionStatus = judoDatasourceFixture
                .getTransactionManager()
                .getTransaction(
                    new DefaultTransactionDefinition(
                        TransactionDefinition.PROPAGATION_MANDATORY
                    )
                );
            judoDatasourceFixture
                .getTransactionManager()
                .commit(transactionStatus);
        } catch (Exception e) {}
    }
}
