package hu.blackbelt.judo.runtime.core.bootstrap;

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
import com.google.inject.name.Names;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.PlatformTransactionManagerProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.SimpleLiquibaseExecutorProvider;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import org.springframework.transaction.PlatformTransactionManager;

public abstract class JudoModule extends AbstractModule {
    public static final String RDBMS_SEQUENCE_START = "rdbmsSequenceStart";
    public static final String RDBMS_SEQUENCE_INCREMENT = "rdbmsSequenceIncrement";
    public static final String RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS = "rdbmsSequenceCreateIfNotExists";

    protected void configure() {
        configureDialect();
        configureMapperFactory();
        configureRdbmsParameterMapper();
        configureDataSource();
        configureSequence();
        configureSequenceCreateIfNotExists();
        configureSequenceStart();
        configureSequenceIncrement();
        configureTransactionManager();
        configureSimpleLiquibaseExecutor();
        configureRdbmsInit();
    }

    protected void configureDialect() {
        throw new IllegalArgumentException("Dialect have to be configured");
    }


    protected void configureMapperFactory() {
        throw new IllegalArgumentException("MapperFactory have to be configured");
    }

    protected void configureRdbmsParameterMapper() {
        throw new IllegalArgumentException("RdbmsParameterMapper have to be configured");
    }

    protected void configureDataSource() {
        throw new IllegalArgumentException("DataSource have to be configured");
    }

    protected void configureSequence() {
        throw new IllegalArgumentException("Sequence have to be configured");
    }

    protected void configureSequenceCreateIfNotExists() {
        bind(Boolean.class).annotatedWith(Names.named(RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS)).toInstance(true);
    }

    protected void configureSequenceIncrement() {
        bind(Long.class).annotatedWith(Names.named(RDBMS_SEQUENCE_INCREMENT)).toInstance(1L);
    }

    protected void configureSequenceStart() {
        bind(Long.class).annotatedWith(Names.named(RDBMS_SEQUENCE_START)).toInstance(1L);
    }

    protected void configureTransactionManager() {
        bind(PlatformTransactionManager.class).toProvider(new PlatformTransactionManagerProvider()).in(Singleton.class);
    }

    protected void configureSimpleLiquibaseExecutor() {
        bind(SimpleLiquibaseExecutor.class).toProvider(SimpleLiquibaseExecutorProvider.class).in(Singleton.class);
    }

    protected void configureRdbmsInit() {
        throw new IllegalArgumentException("RdbmsInit have to be configured");
    }
}
