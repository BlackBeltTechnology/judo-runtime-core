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

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.HsqldbRdbmsParameterMapper;

@SuppressWarnings("rawtypes")
public class HsqldbRdbmsParameterMapperProvider implements Provider<RdbmsParameterMapper> {

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    Dialect dialect;

    @Inject
    JudoModelLoader models;

    @Inject
    IdentifierProvider identifierProvider;

    @SuppressWarnings("unchecked")
	@Override
    public RdbmsParameterMapper get() {
        return HsqldbRdbmsParameterMapper.builder()
                .coercer(dataTypeManager.getCoercer())
                .rdbmsModel(models.getRdbmsModel())
                .identifierProvider(identifierProvider)
                .build();
    }
}
