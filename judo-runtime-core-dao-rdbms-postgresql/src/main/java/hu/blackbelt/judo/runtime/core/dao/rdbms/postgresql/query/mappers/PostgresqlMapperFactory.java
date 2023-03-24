package hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.query.mappers;

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

import hu.blackbelt.judo.meta.query.Function;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.DefaultMapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.RdbmsMapper;

import java.util.Map;

public class PostgresqlMapperFactory<ID> extends DefaultMapperFactory<ID> {

    @Override
    public Map<Class<?>, RdbmsMapper<?>> getMappers(RdbmsBuilder<ID> rdbmsBuilder) {
        Map<Class<?>, RdbmsMapper<?>> mappers = super.getMappers(rdbmsBuilder);
        mappers.put(Function.class, new PostgresqlFunctionMapper<ID>(rdbmsBuilder));
        return mappers;
    }
}
