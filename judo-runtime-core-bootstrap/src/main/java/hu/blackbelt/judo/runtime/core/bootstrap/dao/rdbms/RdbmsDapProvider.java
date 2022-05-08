package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilderConfig;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelSpecification;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsDAOImpl;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;

import javax.sql.DataSource;

public class RdbmsDapProvider implements Provider<DAO> {

    private final DataTypeManager dataTypeManager;

    private final JudoModelSpecification models;
    private final DataSource dataSource;
    private final TransformationTraceService transformationTraceService;
    private final IdentifierProvider identifierProvider;
    private final Dialect dialect;
    private final VariableResolver variableResolver;
    private final Context context;
    private final MetricsCollector metricsCollector;

    /*
    @Builder.Default
    private boolean optimisticLockEnabled = true;

    @Builder.Default
    private int chunkSize = 10;

    @Builder.Default
    private boolean markSelectedRangeItems = false;

     */

    @Inject
    public RdbmsDapProvider(
            DataSource dataSource,
            Dialect dialect,
            Context context,
            DataTypeManager dataTypeManager,
            JudoModelSpecification models,
            TransformationTraceService transformationTraceService,
            IdentifierProvider identifierProvider,
            VariableResolver variableResolver,
            MetricsCollector metricsCollector
    ) {
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.context = context;
        this.dataTypeManager = dataTypeManager;

        this.models = models;
        this.transformationTraceService = transformationTraceService;
        this.identifierProvider = identifierProvider;
        this.variableResolver = variableResolver;
        this.metricsCollector = metricsCollector;
    }

    @Override
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
                .metricsCollector(metricsCollector);

        return builder.build();
    }
}
