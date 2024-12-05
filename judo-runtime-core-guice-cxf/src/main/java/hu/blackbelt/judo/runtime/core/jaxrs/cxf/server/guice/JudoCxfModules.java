package hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.guice;

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
import com.google.inject.Singleton;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.interceptors.ExchangeIdDecorator;
import hu.blackbelt.judo.runtime.core.jaxrs.cxf.server.interceptors.ExchangeIdResponseWriter;
import lombok.Builder;
import org.apache.cxf.rs.security.cors.CrossOriginResourceSharingFilter;

import java.util.Objects;

public class JudoCxfModules extends AbstractModule {

    private Boolean exchangeIdInterceptors;
    private Integer cxfJaxRsServerPort;
    private String cxfJaxRsServerUrl;
    private String cxfJaxRsServerPath;


    public static class JudoCxfModulesBuilder {
        Boolean exchangeIdInterceptors;
        Integer cxfJaxRsServerPort;
        String cxfJaxRsServerUrl;
        String cxfJaxRsServerPath;
    }

    @Builder
    private JudoCxfModules(Integer cxfJaxRsServerPort,
                           String cxfJaxRsServerUrl,
                           String cxfJaxRsServerPath,
                           Boolean exchangeIdInterceptors) {
        this.cxfJaxRsServerPort = Objects.requireNonNullElse(cxfJaxRsServerPort, 8181);
        this.cxfJaxRsServerUrl = Objects.requireNonNullElse(cxfJaxRsServerUrl, "http://localhost");
        this.cxfJaxRsServerPath = Objects.requireNonNullElse(cxfJaxRsServerPath, "http://localhost");
        this.exchangeIdInterceptors = Objects.requireNonNullElse(exchangeIdInterceptors, true);
    }

    protected void configure() {
        configureServer();
        if (exchangeIdInterceptors) {
            configureExchangeInterceptors();
        }
        bind(Integer.class).annotatedWith(CxfConfigurations.CxfJaxRsServerPort.class).toInstance(cxfJaxRsServerPort);
        bind(String.class).annotatedWith(CxfConfigurations.CxfJaxRsServerUrl.class).toInstance(cxfJaxRsServerUrl);
        bind(String.class).annotatedWith(CxfConfigurations.CxfJaxRsServerUrl.class).toInstance(cxfJaxRsServerPath);
    }

    protected void configureServer() {
        bind(CxfJaxrsServerProvider.ServerHolder.class).toProvider(CxfJaxrsServerProvider.class).in(Singleton.class);
    }

    protected void configureExchangeInterceptors() {
        bind(ExchangeIdDecorator.class).toProvider(ExchangeIdDecoratorProvider.class).asEagerSingleton();
        bind(ExchangeIdResponseWriter.class).toProvider(ExchangeIdResponseWriterProvider.class).asEagerSingleton();
        bind(CrossOriginResourceSharingFilter.class).toProvider(CrossOriginResourceSharingFilterProvider.class).asEagerSingleton();
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
