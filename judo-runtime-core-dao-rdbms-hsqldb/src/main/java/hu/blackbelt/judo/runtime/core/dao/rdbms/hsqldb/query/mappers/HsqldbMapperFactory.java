package hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.mappers;

import hu.blackbelt.judo.meta.query.Function;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.DefaultMapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.RdbmsMapper;

import java.util.Map;

public class HsqldbMapperFactory<ID> extends DefaultMapperFactory<ID>  {
	@Override
    public Map<Class<?>, RdbmsMapper<?>> getMappers(RdbmsBuilder<ID> rdbmsBuilder) {
        Map<Class<?>, RdbmsMapper<?>> mappers = super.getMappers(rdbmsBuilder);
        mappers.put(Function.class, new HsqldbFunctionMapper<ID>(rdbmsBuilder));
        return mappers;
    }
}
