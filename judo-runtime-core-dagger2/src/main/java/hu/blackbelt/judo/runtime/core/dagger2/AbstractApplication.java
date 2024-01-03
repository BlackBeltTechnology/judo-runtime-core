package hu.blackbelt.judo.runtime.core.dagger2;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import hu.blackbelt.judo.runtime.core.dispatcher.DispatcherFunctionProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.runtime.core.security.PasswordPolicy;
import hu.blackbelt.judo.runtime.core.security.RealmExtractor;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import org.springframework.transaction.PlatformTransactionManager;

import javax.inject.Inject;
import javax.sql.DataSource;

public abstract class AbstractApplication {

    @Inject
    public SimpleLiquibaseExecutor simpleLiquibaseExecutor;

    @Inject
    public DataTypeManager dataTypeManager;

    @Inject
    public Coercer coercer;

    @Inject
    public ExtendableCoercer extendableCoercer;

    @Inject
    public Context context;

    @Inject
    public IdentifierProvider identifierProvider;

    @Inject
    public DAO dao;

    @Inject
    public Dispatcher dispatcher;

    @Inject AccessManager accessManager;
    public ModifyStatementExecutor modifyStatementExecutor;

    @Inject
    public PlatformTransactionManager platformTransactionManager;

    @Inject
    public QueryFactory queryFactory;

    @Inject
    public RdbmsBuilder rdbmsBuilder;

    @Inject
    public InstanceCollector rdbmsInstanceCollector;

    @Inject
    public RdbmsResolver rdbmsResolver;

    @Inject
    public SelectStatementExecutor selectStatementExecutor;

    @Inject
    public TransformationTraceService transformationTraceService;

    @Inject
    public ActorResolver getActorResolver;

    @Inject
    public IdentifierSigner identifierSigner;

    @Inject
    public MetricsCollector metricsCollector;

    @Inject
    public PayloadValidator payloadValidator;

    @Inject
    public VariableResolver variableResolver;

    @Inject
    public DispatcherFunctionProvider dispatcherFunctionProvider;

    @Inject
    public OperationCallInterceptorProvider operationCallInterceptorProvider;

    @Inject
    public ValidatorProvider validatorProvider;

    @Inject
    public PasswordPolicy passwordPolicy;

    @Inject
    public RealmExtractor realmExtractor;

    @Inject
    public DataSource dataSource;

    @Inject
    public MapperFactory mapperFactory;

    @Inject
    public RdbmsInit rdbmsInit;

    @Inject
    public RdbmsParameterMapper rdbmsParameterMapper;

    @Inject
    public Sequence sequence;

    @Inject
    public Dialect dialect;

}
