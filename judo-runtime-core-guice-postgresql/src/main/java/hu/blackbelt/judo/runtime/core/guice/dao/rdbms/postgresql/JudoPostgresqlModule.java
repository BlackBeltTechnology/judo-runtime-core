package hu.blackbelt.judo.runtime.core.guice.dao.rdbms.postgresql;

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


import javax.sql.DataSource;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.PlatformTransactionManagerProvider;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.PostgresqlDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import lombok.*;
import org.springframework.transaction.PlatformTransactionManager;

public class JudoPostgresqlModule extends AbstractModule {

    @Getter
    private JudoPostgresqlModuleConfiguration configuration;

    public static class JudoPostgresqlModuleBuilder {
        JudoPostgresqlModuleConfiguration configuration = null;
        String host = JudoPostgresqlModuleConfiguration.DEFAULT.getHost();
        Integer port = JudoPostgresqlModuleConfiguration.DEFAULT.getPort();
        String user = JudoPostgresqlModuleConfiguration.DEFAULT.getUser();
        String password = JudoPostgresqlModuleConfiguration.DEFAULT.getPassword();
        String databaseName = JudoPostgresqlModuleConfiguration.DEFAULT.getDatabaseName();
        Integer poolSize = JudoPostgresqlModuleConfiguration.DEFAULT.getPoolSize();
        PlatformTransactionManager platformTransactionManager = JudoPostgresqlModuleConfiguration.DEFAULT.getPlatformTransactionManager();
        MapperFactory mapperFactory = JudoPostgresqlModuleConfiguration.DEFAULT.getMapperFactory();
        RdbmsParameterMapper rdbmsParameterMapper = JudoPostgresqlModuleConfiguration.DEFAULT.getRdbmsParameterMapper();;
        DataSource dataSource = JudoPostgresqlModuleConfiguration.DEFAULT.getDataSource();
        Sequence sequence = JudoPostgresqlModuleConfiguration.DEFAULT.getSequence();
        RdbmsInit rdbmsInit = JudoPostgresqlModuleConfiguration.DEFAULT.getRdbmsInit();
    }

    @Builder
    private JudoPostgresqlModule(JudoPostgresqlModuleConfiguration configuration, 
                                 String host,
                                 Integer port,
                                 String user,
                                 String password,
                                 String databaseName,
                                 Integer poolSize,
                                 PlatformTransactionManager platformTransactionManager,
                                 MapperFactory mapperFactory,
                                 RdbmsParameterMapper rdbmsParameterMapper,
                                 DataSource dataSource,
                                 Sequence sequence,
                                 RdbmsInit rdbmsInit) {
        if (configuration != null) {
            this.configuration = configuration;
        } else {
            this.configuration = JudoPostgresqlModuleConfiguration.builder()
                    .host(host)
                    .port(port)
                    .user(user)
                    .password(password)
                    .databaseName(databaseName)
                    .poolSize(poolSize)
                    .platformTransactionManager(platformTransactionManager)
                    .mapperFactory(mapperFactory)
                    .rdbmsParameterMapper(rdbmsParameterMapper)
                    .dataSource(dataSource)
                    .sequence(sequence)
                    .rdbmsInit(rdbmsInit)
                    .build();
        }
    }
    protected void configure() {
        configureDialect();
        configurePlatformTransactionManager();
        configureOptions();
        configureDataSource();
        configureMapperFactory();
        configureRdbmsInit();
        configureRdbmsParameterMapper();
        configureSequence();
    }


    protected void configureDialect() {
        bind(Dialect.class).toInstance(new PostgresqlDialect());
    }

    protected void configureOptions() {
        bind(Integer.class).annotatedWith(PostgresqlConfiguration.PostgresqlPort.class).toInstance(configuration.getPort());
        bind(String.class).annotatedWith(PostgresqlConfiguration.PostgresqlHost.class).toInstance(configuration.getHost());
        bind(String.class).annotatedWith(PostgresqlConfiguration.PostgresqlUser.class).toInstance(configuration.getUser());
        bind(String.class).annotatedWith(PostgresqlConfiguration.PostgresqlPassword.class).toInstance(configuration.getPassword());
        bind(String.class).annotatedWith(PostgresqlConfiguration.PostgresqlDatabaseName.class).toInstance(configuration.getDatabaseName());
    }


    protected void configurePlatformTransactionManager() {
        if (configuration.getPlatformTransactionManager() != null) {
            bind(PlatformTransactionManager.class).toInstance(configuration.getPlatformTransactionManager());
        } else {
            bind(PlatformTransactionManager.class).toProvider(PlatformTransactionManagerProvider.class).in(Singleton.class);
        }
    }

    protected void configureMapperFactory() {
        if (configuration.getMapperFactory() != null) {
            bind(MapperFactory.class).toInstance(configuration.getMapperFactory());
        } else {
            bind(MapperFactory.class).toProvider(PostgresqlMapperFactoryProvider.class).in(Singleton.class);
        }
    }

    protected void configureRdbmsParameterMapper() {
        if (configuration.getRdbmsParameterMapper() != null) {
            bind(RdbmsParameterMapper.class).toInstance(configuration.getRdbmsParameterMapper());
        } else {
            bind(RdbmsParameterMapper.class).toProvider(PostgresqlRdbmsParameterMapperProvider.class).in(Singleton.class);
        }
    }

    protected void configureDataSource() {
        if (configuration.getDataSource() != null) {
            bind(DataSource.class).toInstance(configuration.getDataSource());
        } else {
            bind(DataSource.class).toProvider(PostgresqlDataSourceProvider.class).in(Singleton.class);
        }
    }

    protected void configureSequence() {
        if (configuration.getSequence() != null) {
            bind(Sequence.class).toInstance(configuration.getSequence());
        } else {
            bind(Sequence.class).toProvider(PostgresqlRdbmsSequenceProvider.class).in(Singleton.class);
        }
    }

    protected void configureRdbmsInit() {
        if (configuration.getRdbmsInit() != null) {
            bind(RdbmsInit.class).toInstance(configuration.getRdbmsInit());
        } else {
            bind(RdbmsInit.class).toProvider(PostgresqlRdbmsInitProvider.class).in(Singleton.class);
        }
    }
}
