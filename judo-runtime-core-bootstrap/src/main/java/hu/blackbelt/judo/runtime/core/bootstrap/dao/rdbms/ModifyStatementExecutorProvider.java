package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

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
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;

import javax.inject.Provider;

@SuppressWarnings("rawtypes")
public class ModifyStatementExecutorProvider implements Provider<ModifyStatementExecutor> {

    @Inject
    AsmModel asmModel;

    @Inject
    RdbmsModel rdbmsModel;

    @Inject
    Coercer coercer;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject
    TransformationTraceService transformationTraceService;

    @Inject
    RdbmsParameterMapper rdbmsParameterMapper;

    @Inject
    RdbmsResolver rdbmsResolver;

    @SuppressWarnings("unchecked")
    @Override
    public ModifyStatementExecutor get() {
        return ModifyStatementExecutor.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .identifierProvider(identifierProvider)
                .transformationTraceService(this.transformationTraceService)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .coercer(coercer)
                .rdbmsResolver(rdbmsResolver)
                .build();
    }
}
