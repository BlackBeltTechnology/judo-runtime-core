package hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms;

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
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static java.util.Objects.requireNonNullElse;

@SuppressWarnings("rawtypes")
@Module
public class SelectStatementExecutorModule {

    public static final String SELECT_CHUNK_SIZE = "rdbmsDaoChunkSize";

    @SuppressWarnings("unchecked")
    @JudoApplicationScope
    @Provides
    public SelectStatementExecutor providesSelectStatementExecutor(
            AsmModel asmModel,
            RdbmsModel rdbmsModel,
            QueryFactory queryFactory,
            DataTypeManager dataTypeManager,
            IdentifierProvider identifierProvider,
            MetricsCollector metricsCollector,
            TransformationTraceService transformationTraceService,
            RdbmsParameterMapper rdbmsParameterMapper,
            RdbmsBuilder rdbmsBuilder,
            RdbmsResolver rdbmsResolver,
            @Named(SELECT_CHUNK_SIZE) @Nullable Integer chunkSize
    ) {
        return SelectStatementExecutor.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .queryFactory(queryFactory)
                .dataTypeManager(dataTypeManager)
                .identifierProvider(identifierProvider)
                .metricsCollector(metricsCollector)
                .chunkSize(requireNonNullElse(chunkSize, 1000))
                .transformationTraceService(transformationTraceService)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .rdbmsBuilder(rdbmsBuilder)
                .queryFactory(queryFactory)
                .rdbmsResolver(rdbmsResolver)
               .build();
    }
}
