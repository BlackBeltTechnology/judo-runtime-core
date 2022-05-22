package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelHolder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.PostgresqlRdbmsParameterMapper;

@SuppressWarnings("rawtypes")
public class PostgresqlRdbmsParameterMapperProvider implements Provider<RdbmsParameterMapper> {

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    Dialect dialect;

    @Inject
    JudoModelHolder models;

    @Inject
    IdentifierProvider identifierProvider;

    @SuppressWarnings("unchecked")
	@Override
    public RdbmsParameterMapper get() {
        return PostgresqlRdbmsParameterMapper.builder()
                .coercer(dataTypeManager.getCoercer())
                .rdbmsModel(models.getRdbmsModel())
                .identifierProvider(identifierProvider)
                .build();
    }
}
