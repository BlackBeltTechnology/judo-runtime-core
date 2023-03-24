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
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;

import javax.annotation.Nullable;
import javax.inject.Provider;

@SuppressWarnings("rawtypes")
public class SelectStatementExecutorProvider implements Provider<SelectStatementExecutor> {

    public static final String SELECT_CHUNK_SIZE = "rdbmsDaoChunkSize";

    @Inject
    AsmModel asmModel;

    @Inject
    RdbmsModel rdbmsModel;

    @Inject
    QueryFactory queryFactory;

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject
    MetricsCollector metricsCollector;

    @Inject
    TransformationTraceService transformationTraceService;

    @Inject
    RdbmsParameterMapper rdbmsParameterMapper;

    @Inject
    RdbmsBuilder rdbmsBuilder;

    @Inject
    RdbmsResolver rdbmsResolver;

    @Inject(optional = true)
    @Named(SELECT_CHUNK_SIZE)
    @Nullable
    private Integer chunkSize = 1000;

    @SuppressWarnings("unchecked")
    @Override
    public SelectStatementExecutor get() {
        return SelectStatementExecutor.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .queryFactory(queryFactory)
                .dataTypeManager(dataTypeManager)
                .identifierProvider(identifierProvider)
                .metricsCollector(metricsCollector)
                .chunkSize(this.chunkSize)
                .transformationTraceService(this.transformationTraceService)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .rdbmsBuilder(rdbmsBuilder)
                .queryFactory(queryFactory)
                .rdbmsResolver(rdbmsResolver)
               .build();
    }
}
