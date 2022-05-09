package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelSpecification;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.HsqldbRdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;

@SuppressWarnings("rawtypes")
public class HsqldbRdbmsParameterMapperProvider implements Provider<RdbmsParameterMapper> {

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    Dialect dialect;

    @Inject
    JudoModelSpecification models;

    @Inject
    IdentifierProvider identifierProvider;

    @Override
    public RdbmsParameterMapper get() {
        return HsqldbRdbmsParameterMapper.hsqldbBuilder()
                .coercer(dataTypeManager.getCoercer())
                .dialect(dialect)
                .rdbmsModel(models.getRdbmsModel())
                .identifierProvider(identifierProvider)
                .build();
    }
}
