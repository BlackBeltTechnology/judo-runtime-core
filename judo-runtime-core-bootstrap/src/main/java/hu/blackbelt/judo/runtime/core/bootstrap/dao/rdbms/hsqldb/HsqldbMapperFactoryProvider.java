package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

import com.google.inject.Provider;

import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.mappers.HsqldbMapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;

public class HsqldbMapperFactoryProvider implements Provider<MapperFactory> {

	@Override
	public MapperFactory get() {
		return new HsqldbMapperFactory();
	}

}
