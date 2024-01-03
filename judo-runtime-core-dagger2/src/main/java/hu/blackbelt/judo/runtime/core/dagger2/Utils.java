package hu.blackbelt.judo.runtime.core.dagger2;

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.ExtendableCoercer;

public interface Utils {
    SimpleLiquibaseExecutor getSimpleLiquibaseExecutor();
    DataTypeManager getDataTypeManager();
    Coercer getCoercer();
    ExtendableCoercer getExtendableCoercer();
    Context getContext();
    IdentifierProvider getIdentifierProvider();
}
