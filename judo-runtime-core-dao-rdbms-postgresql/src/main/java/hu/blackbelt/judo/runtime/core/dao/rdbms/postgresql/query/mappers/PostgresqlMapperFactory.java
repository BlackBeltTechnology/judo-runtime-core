package hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.query.mappers;

import hu.blackbelt.judo.meta.query.Function;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.DefaultMapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.RdbmsMapper;

import java.util.Map;

public class PostgresqlMapperFactory<ID> extends DefaultMapperFactory<ID> {

    @Override
    public Map<Class<?>, RdbmsMapper<?>> getMappers(RdbmsBuilder<ID> rdbmsBuilder) {
        Map<Class<?>, RdbmsMapper<?>> mappers = super.getMappers(rdbmsBuilder);
        mappers.put(Function.class, new PostgresqlFunctionMapper<ID>(rdbmsBuilder));
        return mappers;
    }
}
