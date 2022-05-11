package hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.mappers;

import hu.blackbelt.judo.meta.query.Function;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.DefaultMapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.FunctionMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.RdbmsMapper;

import java.util.Map;

public class HsqldbMapperFactory extends DefaultMapperFactory  {
    @Override
    public Map<Class, RdbmsMapper> getMappers(RdbmsBuilder rdbmsBuilder) {
        Map<Class, RdbmsMapper> mappers = super.getMappers(rdbmsBuilder);
        mappers.put(Function.class, new HsqldbFunctionMapper(rdbmsBuilder));
        return mappers;
    }
}
