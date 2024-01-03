package hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms.hsqldb;

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

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbRdbmsSequence;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.sql.DataSource;

import static hu.blackbelt.judo.runtime.core.dagger2.database.Database.*;
import static java.util.Objects.requireNonNullElse;

@SuppressWarnings("rawtypes")
@Module
public class HsqldbRdbmsSequenceModule {

    @JudoApplicationScope
    @Provides
    public Sequence providesSequence(
            DataSource dataSource,
            @Named(RDBMS_SEQUENCE_START) @Nullable Long start,
            @Named(RDBMS_SEQUENCE_INCREMENT) @Nullable Long increment,
            @Named(RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS) @Nullable Boolean createIfNotExists) {
        HsqldbRdbmsSequence.HsqldbRdbmsSequenceBuilder builder = HsqldbRdbmsSequence.builder();
        builder
                .dataSource(dataSource)
                .start(requireNonNullElse(start, 1L))
                .increment(requireNonNullElse(increment, 1L))
                .createIfNotExists(requireNonNullElse(createIfNotExists, true));

        return builder.build();
    }
}
