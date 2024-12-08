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

import javax.sql.DataSource;

import com.google.inject.AbstractModule;
import com.google.inject.util.Providers;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.PlatformTransactionManagerProvider;
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
        Boolean runServer = JudoHsqldbModuleConfiguration.DEFAULT.getRunServer();
        String databaseName = JudoHsqldbModuleConfiguration.DEFAULT.getDatabaseName();
        File databasePath = JudoHsqldbModuleConfiguration.DEFAULT.getDatabasePath();
        Integer port = JudoHsqldbModuleConfiguration.DEFAULT.getPort();
    }

    @Builder
    private JudoHsqldbModule(JudoHsqldbModuleConfiguration configuration,
                             Boolean runServer,
                             String databaseName,
                             File databasePath,
                             Integer port) {

        if (configuration != null) {
            this.configuration = configuration;
        } else {
            this.configuration = JudoHsqldbModuleConfiguration.builder()
                    .port(port)
                    .runServer(runServer)
                    .databaseName(databaseName)
                    .databasePath(databasePath)
                    .build();
        }
    }
    protected void configure() {
        super.configure();

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
        bind(Integer.class).annotatedWith(HsqlDbConfigurationQualifier.HsqldbServerPort.class).toInstance(configuration.getPort());
        bind(String.class).annotatedWith(HsqlDbConfigurationQualifier.HsqldbServerDatabaseName.class).toInstance(configuration.getDatabaseName());
        bind(File.class).annotatedWith(HsqlDbConfigurationQualifier.HsqldbServerDatabasePath.class).toInstance(configuration.getDatabasePath());
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
        bind(PlatformTransactionManager.class).toProvider(PlatformTransactionManagerProvider.class).in(Singleton.class);
    }

    protected void configureMapperFactory() {
        bind(MapperFactory.class).toProvider(HsqldbMapperFactoryProvider.class).in(Singleton.class);
    }

    protected void configureRdbmsParameterMapper() {
        bind(RdbmsParameterMapper.class).toProvider(HsqldbRdbmsParameterMapperProvider.class).in(Singleton.class);
    }

    protected void configureDataSource() {
        bind(DataSource.class).toProvider(HsqldbDataSourceProvider.class).in(Singleton.class);
    }

    protected void configureSequence() {
        bind(Sequence.class).toProvider(HsqldbRdbmsSequenceProvider.class).in(Singleton.class);
    }

    protected void configureRdbmsInit() {
        bind(RdbmsInit.class).toProvider(HsqldbRdbmsInitProvider.class).in(Singleton.class);
    }
}
