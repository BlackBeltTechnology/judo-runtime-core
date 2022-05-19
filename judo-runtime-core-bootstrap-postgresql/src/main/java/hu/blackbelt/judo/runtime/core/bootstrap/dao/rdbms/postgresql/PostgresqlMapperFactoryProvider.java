package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql;

import com.google.inject.Provider;

import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.query.mappers.PostgresqlMapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;

@SuppressWarnings("rawtypes")
public class PostgresqlMapperFactoryProvider implements Provider<MapperFactory> {

	@Override
	public MapperFactory get() {
		return new PostgresqlMapperFactory();
	}

}
