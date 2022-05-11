package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;

import java.util.Map;

public interface MapperFactory {
    Map<Class, RdbmsMapper> getMappers(RdbmsBuilder rdbmsBuilder);
}
