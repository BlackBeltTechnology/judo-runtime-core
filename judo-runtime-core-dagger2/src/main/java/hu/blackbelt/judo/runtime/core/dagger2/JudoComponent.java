package hu.blackbelt.judo.runtime.core.dagger2;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
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

import javax.sql.DataSource;


public interface JudoComponent {
    AccessManager getAccessManager();
    ModifyStatementExecutor geModifyStatementExecutor();
    PlatformTransactionManager gePlatformTransactionManager();
    QueryFactory getQueryFactory();
    RdbmsBuilder getRdbmsBuilder();
    DAO getDao();
    InstanceCollector getRdbmsInstanceCollector();
    RdbmsResolver getRdbmsResolver();
    SelectStatementExecutor getSelectStatementExecutor();
    TransformationTraceService getTransformationTraceService();
    ActorResolver getActorResolver();
    Dispatcher getDispatcher();
    IdentifierSigner getIdentifierSigner();
    MetricsCollector getMetricsCollector();
    PayloadValidator getPayloadValidator();
    VariableResolver getVariableResolver();
    DispatcherFunctionProvider getDispatcherFunctionProvider();
    OperationCallInterceptorProvider getOperationCallInterceptorProvider();
    ValidatorProvider getValidatorProvider();
    PasswordPolicy getPasswordPolicy();
    RealmExtractor getRealmExtractor();
}
