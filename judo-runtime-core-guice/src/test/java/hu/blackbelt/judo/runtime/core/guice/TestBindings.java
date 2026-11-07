package hu.blackbelt.judo.runtime.core.guice;

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
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import org.mockito.Mockito;

import javax.sql.DataSource;

/**
 * Test helper. The bindings a database module would provide, so that the injector of {@link JudoDefaultModule} validates
 * without a database.
 */
final class TestBindings extends AbstractModule {
    @Override
    protected void configure() {
        bind(Dialect.class).toInstance(new HsqldbDialect());
        bind(DataSource.class).toInstance(Mockito.mock(DataSource.class));
        bind(RdbmsInit.class).toInstance(Mockito.mock(RdbmsInit.class));
        bind(RdbmsParameterMapper.class).toInstance(Mockito.mock(RdbmsParameterMapper.class));
        bind(MapperFactory.class).toInstance(Mockito.mock(MapperFactory.class));
        bind(Sequence.class).toInstance(Mockito.mock(Sequence.class));
    }
}
