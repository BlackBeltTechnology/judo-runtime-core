package hu.blackbelt.judo.runtime.core.dao.rdbms.fixture;

import liquibase.pro.packaged.P;
import org.junit.jupiter.api.extension.*;

import javax.transaction.Status;

public class JudoDatasourceByClassExtension implements BeforeAllCallback, AfterAllCallback, AfterEachCallback, ParameterResolver {

    private JudoDatasourceFixture  rdbmsDatasourceFixture =  new JudoDatasourceFixture();

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        rdbmsDatasourceFixture.setupDatabase();
        rdbmsDatasourceFixture.prepareDatasources();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        rdbmsDatasourceFixture.teardownDatasource();
    }


    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().isAssignableFrom(JudoDatasourceFixture.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return rdbmsDatasourceFixture;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        if (rdbmsDatasourceFixture.getTransactionManager().getStatus() == Status.STATUS_ACTIVE) {
            rdbmsDatasourceFixture.getTransactionManager().commit();
        }
    }

}
