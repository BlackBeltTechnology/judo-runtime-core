package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

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

import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModule;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbRdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import lombok.Builder;
import org.hsqldb.server.Server;

import javax.sql.DataSource;
import java.io.File;

import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.HsqldbServerProvider.*;

public class JudoHsqldbTestModules extends JudoModule {

    private Boolean runServer = false;
    private String databaseName = "judo";
    private File databasePath = new File(".", "judo.db");
    private Integer port = 31001;

    private DataSource dataSource;

    private HsqldbRdbmsInit hsqldbRdbmsInit;


    public static class JudoHsqldbTestModulesBuilder {
        private Boolean runServer = false;
        private String databaseName = "judo";
        private File databasePath = new File(".", "judo.db");
        private Integer port = 31001;
    }
    @Builder
    private JudoHsqldbTestModules(Boolean runServer, String databaseName, File databasePath, Integer port, DataSource dataSource, HsqldbRdbmsInit hsqldbRdbmsInit) {
        this.runServer = runServer;
        this.databaseName = databaseName;
        this.databasePath = databasePath;
        this.port = port;
        this.dataSource = dataSource;
        this.hsqldbRdbmsInit = hsqldbRdbmsInit;
    }
    protected void configure() {
        super.configure();

        // HSQLDB
        if (runServer) {
            bind(Server.class).toProvider(HsqldbServerProvider.class).in(Singleton.class);
        } else {
            bind(Server.class).toProvider(Providers.of(null)).in(Singleton.class);
        }

        bind(String.class).annotatedWith(Names.named(HSQLDB_SERVER_DATABASE_NAME)).toInstance(databaseName);
        bind(File.class).annotatedWith(Names.named(HSQLDB_SERVER_DATABASE_PATH)).toInstance(databasePath);
        bind(Integer.class).annotatedWith(Names.named(HSQLDB_SERVER_PORT)).toInstance(port);

    }

    @Override
    protected void configureDialect() {
        bind(Dialect.class).toInstance(new HsqldbDialect());
    }

    @Override
    protected void configureMapperFactory() {
        bind(MapperFactory.class).toProvider(HsqldbMapperFactoryProvider.class).in(Singleton.class);
    }

    @Override
    protected void configureRdbmsParameterMapper() {
        bind(RdbmsParameterMapper.class).toProvider(HsqldbRdbmsParameterMapperProvider.class).in(Singleton.class);
    }

    @Override
    protected void configureDataSource() {
        if(dataSource != null) {
            bind(DataSource.class).toInstance(dataSource);
        } else {
            bind(DataSource.class).toProvider(HsqldbDataSourceProvider.class).in(Singleton.class);
        }
    }

    @Override
    protected void configureSequence() {
        bind(Sequence.class).toProvider(HsqldbRdbmsSequenceProvider.class).in(Singleton.class);
    }

    @Override
    protected void configureRdbmsInit() {
        if(hsqldbRdbmsInit != null) {
            bind(RdbmsInit.class).toInstance(hsqldbRdbmsInit);
        } else {
            bind(RdbmsInit.class).toProvider(HsqldbRdbmsInitProvider.class).in(Singleton.class);
        }
    }
}
