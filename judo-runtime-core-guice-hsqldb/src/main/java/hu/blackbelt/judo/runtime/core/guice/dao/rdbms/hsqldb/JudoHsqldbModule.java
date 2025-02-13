package hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb;

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
import java.io.File;
import java.util.Objects;

import javax.sql.DataSource;

import com.google.inject.AbstractModule;
import com.google.inject.util.Providers;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.PlatformTransactionManagerProvider;
import hu.blackbelt.judo.runtime.core.utils.RuntimeVariableResolver;
import lombok.*;
import org.hsqldb.server.Server;

import com.google.inject.Singleton;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import org.springframework.transaction.PlatformTransactionManager;

public class JudoHsqldbModule extends AbstractModule {

    @Getter
    private JudoHsqldbModuleConfiguration configuration;

    public static class JudoHsqldbModuleBuilder {
        JudoHsqldbModuleConfiguration configuration = null;
        RuntimeVariableResolver runtimeVariableResolver = null;
        Boolean runServer = JudoHsqldbModuleConfiguration.DEFAULT.getRunServer();
        String databaseName = JudoHsqldbModuleConfiguration.DEFAULT.getDatabaseName();
        File databasePath = JudoHsqldbModuleConfiguration.DEFAULT.getDatabasePath();
        Integer port = JudoHsqldbModuleConfiguration.DEFAULT.getPort();
        PlatformTransactionManager platformTransactionManager = JudoHsqldbModuleConfiguration.DEFAULT.getPlatformTransactionManager();
        MapperFactory mapperFactory = JudoHsqldbModuleConfiguration.DEFAULT.getMapperFactory();
        RdbmsParameterMapper rdbmsParameterMapper = JudoHsqldbModuleConfiguration.DEFAULT.getRdbmsParameterMapper();
        DataSource dataSource = JudoHsqldbModuleConfiguration.DEFAULT.getDataSource();
        Sequence sequence = JudoHsqldbModuleConfiguration.DEFAULT.getSequence();
        RdbmsInit rdbmsInit = JudoHsqldbModuleConfiguration.DEFAULT.getRdbmsInit();
    }

    @Builder
    private JudoHsqldbModule(JudoHsqldbModuleConfiguration configuration,
                             RuntimeVariableResolver runtimeVariableResolver,
                             Boolean runServer,
                             String databaseName,
                             File databasePath,
                             Integer port,
                             PlatformTransactionManager platformTransactionManager,
                             MapperFactory mapperFactory,
                             RdbmsParameterMapper rdbmsParameterMapper,
                             DataSource dataSource,
                             Sequence sequence,
                             RdbmsInit rdbmsInit) {

        if (configuration != null) {
            this.configuration = configuration;
        } else {
            this.configuration = JudoHsqldbModuleConfiguration.builder()
                    .runtimeVariableResolver(Objects.requireNonNullElseGet(runtimeVariableResolver,
                            () -> RuntimeVariableResolver.builder().prefix("judo").build()))
                    .port(port)
                    .runServer(runServer)
                    .databaseName(databaseName)
                    .databasePath(databasePath)
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
        configureMapperFactory();
        configureRdbmsParameterMapper();
        configureDataSource();
        configureSequence();
        configureRdbmsInit();
        configureServer();
        configurePlatformTransactionManager();
    }

    protected void configureOptions() {
        RuntimeVariableResolver runtimeVariableResolver = Objects.requireNonNull(configuration.getRuntimeVariableResolver(), "RuntimeVariableResolver must not be null");

        bind(Integer.class).annotatedWith(HsqlDbConfigurationQualifier.HsqldbServerPort.class)
                .toInstance(runtimeVariableResolver
                        .getVariableAsInteger("hsqldbServerPort",
                                configuration.getPort()));

        bind(String.class).annotatedWith(HsqlDbConfigurationQualifier.HsqldbServerDatabaseName.class)
                .toInstance(runtimeVariableResolver
                        .getVariableAsString("hsqldbServerDatabaseName",
                                configuration.getDatabaseName()));

        bind(File.class).annotatedWith(HsqlDbConfigurationQualifier.HsqldbServerDatabasePath.class)
                .toInstance(runtimeVariableResolver
                        .getVariableAsFile("hsqldbServerDatabasePath",
                                configuration.getDatabasePath()));
    }

    protected void configureServer() {
        if (configuration.getRunServer()) {
            bind(Server.class).toProvider(HsqldbServerProvider.class).in(Singleton.class);
        } else {
            bind(Server.class).toProvider(Providers.of(null)).in(Singleton.class);
        }
    }

    protected void configureDialect() {
        bind(Dialect.class).toInstance(new HsqldbDialect());
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
            bind(MapperFactory.class).toProvider(HsqldbMapperFactoryProvider.class).in(Singleton.class);
        }
    }

    protected void configureRdbmsParameterMapper() {
        if (configuration.getRdbmsParameterMapper() != null) {
            bind(RdbmsParameterMapper.class).toInstance(configuration.getRdbmsParameterMapper());
        } else {
            bind(RdbmsParameterMapper.class).toProvider(HsqldbRdbmsParameterMapperProvider.class).in(Singleton.class);
        }
    }

    protected void configureDataSource() {
        if (configuration.getDataSource() != null) {
            bind(DataSource.class).toInstance(configuration.getDataSource());
        } else {
            bind(DataSource.class).toProvider(HsqldbDataSourceProvider.class).in(Singleton.class);
        }
    }

    protected void configureSequence() {
        if (configuration.getSequence() != null) {
            bind(Sequence.class).toInstance(configuration.getSequence());
        } else {
            bind(Sequence.class).toProvider(HsqldbRdbmsSequenceProvider.class).in(Singleton.class);
        }
    }

    protected void configureRdbmsInit() {
        if (configuration.getRdbmsInit() != null) {
            bind(RdbmsInit.class).toInstance(configuration.getRdbmsInit());
        } else {
            bind(RdbmsInit.class).toProvider(HsqldbRdbmsInitProvider.class).in(Singleton.class);
        }
    }
}
