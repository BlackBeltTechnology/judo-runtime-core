package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */


import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql.PostgresqlRdbmsSequenceProvider.RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS;
import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql.PostgresqlRdbmsSequenceProvider.RDBMS_SEQUENCE_INCREMENT;
import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql.PostgresqlRdbmsSequenceProvider.RDBMS_SEQUENCE_START;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.AtomikosUserTransactionManagerProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.SimpleLiquibaseExecutorProvider;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.PostgresqlDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import lombok.Builder;

public class JudoPostgresqlModules extends AbstractModule {
	
	String host = "localhost";
	Integer port = 5432;
	String user = "judo";
	String password = "judo";
	String databaseName = "judo";
	Integer poolSize = 10;

    public static class JudoPostgresqlModulesBuilder {
    	String host = "localhost";
    	Integer port = 5432;
    	String user = "judo";
    	String password = "judo";
    	String databaseName = "judo";
    	Integer poolSize = 10;
    }
	
	@Builder
	private JudoPostgresqlModules(String host, Integer port, String user, String password, String databaseName, Integer poolSize) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.databaseName = databaseName;
		this.poolSize = poolSize;		
	}

	protected void configure() {
        bind(Dialect.class).toInstance(new PostgresqlDialect());
		bind(DataSource.class).toProvider(PostgresqlAtomikosDataSourceProvider.class).in(Singleton.class);

        bind(MapperFactory.class).toProvider(PostgresqlMapperFactoryProvider.class).in(Singleton.class);
        bind(RdbmsParameterMapper.class).toProvider(PostgresqlRdbmsParameterMapperProvider.class).in(Singleton.class);

        bind(Integer.class).annotatedWith(Names.named(PostgresqlAtomikosDataSourceProvider.POSTGRESQL_PORT)).toInstance(port);
        bind(String.class).annotatedWith(Names.named(PostgresqlAtomikosDataSourceProvider.POSTGRESQL_HOST)).toInstance(host);
        bind(String.class).annotatedWith(Names.named(PostgresqlAtomikosDataSourceProvider.POSTGRESQL_USER)).toInstance(user);        
        bind(String.class).annotatedWith(Names.named(PostgresqlAtomikosDataSourceProvider.POSTGRESQL_PASSWORD)).toInstance(password);
        bind(String.class).annotatedWith(Names.named(PostgresqlAtomikosDataSourceProvider.POSTGRESQL_DATABASENAME)).toInstance(databaseName);
        bind(Integer.class).annotatedWith(Names.named(PostgresqlAtomikosDataSourceProvider.POSTGRESQL_POOLSIZE)).toInstance(poolSize);
        
        bind(Sequence.class).toProvider(PostgresqlRdbmsSequenceProvider.class).in(Singleton.class);
        bind(Long.class).annotatedWith(Names.named(RDBMS_SEQUENCE_START)).toInstance(1L);
        bind(Long.class).annotatedWith(Names.named(RDBMS_SEQUENCE_INCREMENT)).toInstance(1L);
        bind(Boolean.class).annotatedWith(Names.named(RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS)).toInstance(true);
        
        bind(TransactionManager.class).toProvider(AtomikosUserTransactionManagerProvider.class).in(Singleton.class);
		bind(SimpleLiquibaseExecutor.class).toProvider(SimpleLiquibaseExecutorProvider.class).in(Singleton.class);
		bind(RdbmsInit.class).toProvider(PostgresqlRdbmsInitProvider.class).in(Singleton.class);
	}

}
