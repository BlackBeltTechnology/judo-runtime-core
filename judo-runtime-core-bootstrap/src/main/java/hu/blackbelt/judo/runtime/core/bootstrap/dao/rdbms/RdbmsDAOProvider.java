package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelSpecification;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsDAOImpl;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;

import javax.sql.DataSource;

@SuppressWarnings("rawtypes")
public class RdbmsDAOProvider implements Provider<DAO> {
    public static final String RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED = "rdbmsDaoOptimisticLockEnabled";
    public static final String RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS = "rdbmsDaoMarkSelectedRangeItems";

    @Inject
    private DataTypeManager dataTypeManager;

    @Inject
    private JudoModelSpecification models;

    @Inject
    private DataSource dataSource;

    @Inject
    private IdentifierProvider identifierProvider;

    @Inject
    private Context context;

    @Inject
    private MetricsCollector metricsCollector;

    @Inject
    private InstanceCollector instanceCollector;

    @Inject
    private ModifyStatementExecutor modifyStatementExecutor;

    @Inject
    private SelectStatementExecutor selectStatementExecutor;

    @Inject
    private QueryFactory queryFactory;

    @Inject
    private RdbmsResolver rdbmsResolver;

    @Inject(optional = true)
    @Named(RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED)
    private Boolean optimisticLockEnabled = true;

    @Inject(optional = true)
    @Named(RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS)
    private Boolean markSelectedRangeItems = false;

    @Override
    @SuppressWarnings("unchecked")
    public DAO get() {
        RdbmsDAOImpl.RdbmsDAOImplBuilder builder =  RdbmsDAOImpl.builder()
                .dataSource(dataSource)
                .context(context)
                .dataTypeManager(dataTypeManager)
                .asmModel(models.getAsmModel())
                .identifierProvider(identifierProvider)
                .instanceCollector(instanceCollector)
                .metricsCollector(metricsCollector)
                .optimisticLockEnabled(optimisticLockEnabled)
                .markSelectedRangeItems(markSelectedRangeItems)
                .selectStatementExecutor(selectStatementExecutor)
                .modifyStatementExecutor(modifyStatementExecutor)
                .queryFactory(queryFactory)
                ;

        return builder.build();
    }
}
