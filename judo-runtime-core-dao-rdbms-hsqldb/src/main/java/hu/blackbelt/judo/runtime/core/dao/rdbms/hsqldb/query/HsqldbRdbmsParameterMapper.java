package hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query;

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

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.DefaultRdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;

import java.sql.Time;

public class HsqldbRdbmsParameterMapper<ID> extends DefaultRdbmsParameterMapper<ID> implements RdbmsParameterMapper<ID> {
    @Builder
    public HsqldbRdbmsParameterMapper(@NonNull Coercer coercer,
                                      @NonNull RdbmsModel rdbmsModel,
                                      @NonNull IdentifierProvider<ID> identifierProvider) {
        super(coercer, rdbmsModel, identifierProvider);

        getSqlTypes().put(Time.class, vd -> "TIMESTAMP");
        getSqlTypes().put(String.class, vd -> "LONGVARCHAR");
    }
}
