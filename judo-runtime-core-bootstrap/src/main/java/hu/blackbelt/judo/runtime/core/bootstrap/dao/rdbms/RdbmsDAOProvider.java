package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelSpecification;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsDAOImpl;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;

import javax.sql.DataSource;

@SuppressWarnings("rawtypes")
public class RdbmsDAOProvider implements Provider<DAO> {
    public static final String RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED = "rdbmsDaoOptimisticLockEnabled";
    public static final String RDBMS_DAO_CHUNK_SIZE = "rdbmsDaoChunkSize";
    public static final String RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS = "rdbmsDaoMarkSelectedRangeItems";

    @Inject
    private DataTypeManager dataTypeManager;

    @Inject
    private JudoModelSpecification models;

    @Inject
    private DataSource dataSource;

    @Inject
    private TransformationTraceService transformationTraceService;

    @Inject
    private IdentifierProvider identifierProvider;

    @Inject
    private Dialect dialect;

    @Inject
    private VariableResolver variableResolver;

    @Inject
    private Context context;

    @Inject
    private MetricsCollector metricsCollector;

    @Inject
    private RdbmsParameterMapper rdbmsParameterMapper;

    @Inject
    private InstanceCollector instanceCollector;

    @Inject(optional = true)
    @Named(RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED)
    private Boolean optimisticLockEnabled = true;

    @Inject(optional = true)
    @Named(RDBMS_DAO_CHUNK_SIZE)
    private Integer chunkSize = 1000;

    @Inject(optional = true)
    @Named(RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS)
    private Boolean markSelectedRangeItems = false;

    @Override
    @SuppressWarnings("unchecked")
    public DAO get() {
        RdbmsDAOImpl.RdbmsDAOImplBuilder builder =  RdbmsDAOImpl.builder()
                .dataSource(dataSource)
                .dialect(dialect)
                .context(context)
                .dataTypeManager(dataTypeManager)
                .asmModel(models.getAsmModel())
                .rdbmsModel(models.getRdbmsModel())
                .measureModel(models.getMeasureModel())
                .transformationTraceService(transformationTraceService)
                .identifierProvider(identifierProvider)
                .variableResolver(variableResolver)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .instanceCollector(instanceCollector)
                .metricsCollector(metricsCollector)
                .optimisticLockEnabled(optimisticLockEnabled)
                .chunkSize(chunkSize)
                .markSelectedRangeItems(markSelectedRangeItems);

        return builder.build();
    }
}
