package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelSpecification;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.HsqldbRdbmsParameterMapper;

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
        return HsqldbRdbmsParameterMapper.builder()
                .coercer(dataTypeManager.getCoercer())
                .rdbmsModel(models.getRdbmsModel())
                .identifierProvider(identifierProvider)
                .build();
    }
}
