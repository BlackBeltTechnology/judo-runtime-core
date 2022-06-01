package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

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
