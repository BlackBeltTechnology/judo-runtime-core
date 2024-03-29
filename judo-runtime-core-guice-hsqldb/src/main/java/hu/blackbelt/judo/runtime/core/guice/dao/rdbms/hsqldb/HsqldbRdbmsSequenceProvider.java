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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbRdbmsSequence;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import static hu.blackbelt.judo.runtime.core.guice.JudoModule.*;

@SuppressWarnings("rawtypes")
public class HsqldbRdbmsSequenceProvider implements Provider<Sequence> {

    @Inject
    private DataSource dataSource;

    @Inject(optional = true)
    @Named(RDBMS_SEQUENCE_START)
    @Nullable
    Long start = 1L;

    @Inject(optional = true)
    @Named(RDBMS_SEQUENCE_INCREMENT)
    @Nullable
    Long increment = 1L;

    @Inject(optional = true)
    @Named(RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS)
    @Nullable
    Boolean createIfNotExists = true;

    @Override
    public Sequence get() {
        HsqldbRdbmsSequence.HsqldbRdbmsSequenceBuilder builder = HsqldbRdbmsSequence.builder();
        builder
                .dataSource(dataSource)
                .start(start)
                .increment(increment)
                .createIfNotExists(createIfNotExists);

        return builder.build();
    }
}
