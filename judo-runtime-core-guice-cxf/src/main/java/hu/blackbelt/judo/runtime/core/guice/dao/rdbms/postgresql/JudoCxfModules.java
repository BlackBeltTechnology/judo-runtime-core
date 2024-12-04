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


import com.google.inject.AbstractModule;
import lombok.Builder;

public class JudoCxfModules extends AbstractModule {

    Integer port = 8181;

    public static class JudoCxfModulesBuilder {
        Integer port = 8181;
    }

    @Builder
    private JudoCxfModules(Integer port) {
        this.port = port;
    }

    /*
    @Override
    protected void configureDialect() {
        bind(Dialect.class).toInstance(new PostgresqlDialect());
    }

    @Override
    protected void configureMapperFactory() {
        bind(MapperFactory.class).toProvider(PostgresqlMapperFactoryProvider.class).in(Singleton.class);
    }

    @Override
    protected void configureRdbmsParameterMapper() {
        bind(RdbmsParameterMapper.class).toProvider(PostgresqlRdbmsParameterMapperProvider.class).in(Singleton.class);
    }

    @Override
    protected void configureDataSource() {
        bind(DataSource.class).toProvider(PostgresqlDataSourceProvider.class).in(Singleton.class);
    }

    @Override
    protected void configureSequence() {
        bind(Sequence.class).toProvider(PostgresqlRdbmsSequenceProvider.class).in(Singleton.class);
    }

    @Override
    protected void configureTransactionManager() {
        bind(Integer.class).annotatedWith(Names.named(PostgresqlDataSourceProvider.POSTGRESQL_PORT)).toInstance(port);
        bind(String.class).annotatedWith(Names.named(PostgresqlDataSourceProvider.POSTGRESQL_HOST)).toInstance(host);
        bind(String.class).annotatedWith(Names.named(PostgresqlDataSourceProvider.POSTGRESQL_USER)).toInstance(user);
        bind(String.class).annotatedWith(Names.named(PostgresqlDataSourceProvider.POSTGRESQL_PASSWORD)).toInstance(password);
        bind(String.class).annotatedWith(Names.named(PostgresqlDataSourceProvider.POSTGRESQL_DATABASENAME)).toInstance(databaseName);
        bind(PlatformTransactionManager.class).toProvider(new PlatformTransactionManagerProvider()).in(Singleton.class);
    }

    @Override
    protected void configureRdbmsInit() {
        bind(RdbmsInit.class).toProvider(PostgresqlRdbmsInitProvider.class).in(Singleton.class);
    }
    */
}
