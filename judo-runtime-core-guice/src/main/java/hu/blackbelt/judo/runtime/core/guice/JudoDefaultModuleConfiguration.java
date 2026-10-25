package hu.blackbelt.judo.runtime.core.guice;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AuthenticationInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dispatcher.DispatcherFunctionProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.Export;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel;
import hu.blackbelt.judo.runtime.core.guice.dispatcher.DefaultPayloadValidatorProvider;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import lombok.*;
import org.eclipse.emf.ecore.EReference;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class JudoDefaultModuleConfiguration {
    public static final JudoDefaultModuleConfiguration DEFAULT = JudoDefaultModuleConfiguration.builder().build();

    @Builder.Default
    Object injectModulesTo = false;
    @Builder.Default
    JudoModelLoader judoModelLoader = null;
    @Builder.Default
    Boolean bindModelHolder = true;
    @Builder.Default
    Map<EReference, CustomJoinDefinition> queryFactoryCustomJoinDefinitions = new ConcurrentHashMap<>();
    @Builder.Default
    Boolean rdbmsDaoOptimisticLockEnabled = true;
    @Builder.Default
    Boolean rdbmsDaoMarkSelectedRangeItems = false;
    @Builder.Default
    Integer rdbmsDaoChunkSize = 1000;
    @Builder.Default
    Integer rdbmsDaoMaximumRecursionCount = 3;
    @Builder.Default
    Boolean actorResolverCheckMappedActors = false;
    @Builder.Default
    String actorResolverAcceptableClients = null;
    @Builder.Default
    String actorResolverPrincipalLocaleAttribute = null;
    @Builder.Default
    String actorResolverSupportedLanguages = null;
    @Builder.Default
    String actorResolverDefaultLanguage = null;
    @Builder.Default
    LocaleResolutionLevel actorResolverLocaleResolutionLevel = LocaleResolutionLevel.BROWSER;
    @Builder.Default
    Boolean dispatcherMetricsReturned = false;
    @Builder.Default
    Boolean dispatcherEnableDefaultValidation = true;
    @Builder.Default
    Boolean dispatcherCaseInsensitiveLike = false;
    @Builder.Default
    Boolean dispatcherTrimString = false;
    @Builder.Default
    String identifierSignerSecret = null;
    @Builder.Default
    Consumer metricsCollectorConsumer = (m) -> {};
    @Builder.Default
    Boolean metricsCollectorEnabled = false;
    @Builder.Default
    Boolean metricsCollectorVerbose = false;
    @Builder.Default
    String payloadValidatorRequiredStringValidatorOption = DefaultPayloadValidatorProvider.ACCEPT_NON_EMPTY;
    @Builder.Default
    Boolean threadContextDebugThreadFork = false;
    @Builder.Default
    Boolean threadContextInheritableContext = true;
    @Builder.Default
    Long rdbmsSequenceStart = 1L;
    @Builder.Default
    Long rdbmsSequenceIncrement = 1L;
    @Builder.Default
    Boolean rdbmsSequenceCreateIfNotExists = true;
    @Builder.Default
    RdbmsResolver rdbmsResolver = null;
    @Builder.Default
    VariableResolver variableResolver = null;
    @Builder.Default
    RdbmsBuilder rdbmsBuilder = null;
    @Builder.Default
    QueryFactory queryFactory = null;
    @Builder.Default
    SelectStatementExecutor selectStatementExecutor = null;
    @Builder.Default
    ModifyStatementExecutor modifyStatementExecutor = null;
    @Builder.Default
    ExtendableCoercer extendableCoercer = null;
    @Builder.Default
    DataTypeManager dataTypeManager = null;
    @Builder.Default
    IdentifierProvider identifierProvider = null;
    @Builder.Default
    IdentifierSigner identifierSigner = null;
    @Builder.Default
    AccessManager accessManager = null;
    @Builder.Default
    AuthenticationInterceptorProvider authenticationInterceptorProvider = null;
    @Builder.Default
    Context context = null;
    @Builder.Default
    MetricsCollector metricsCollector = null;
    @Builder.Default
    TransformationTraceService transformationTraceService = null;
    @Builder.Default
    InstanceCollector instanceCollector = null;
    @Builder.Default
    DAO dao = null;
    @Builder.Default
    ActorResolver actorResolver = null;
    @Builder.Default
    DispatcherFunctionProvider dispatcherFunctionProvider = null;
    @Builder.Default
    OperationCallInterceptorProvider operationCallInterceptorProvider = null;
    @Builder.Default
    Dispatcher dispatcher = null;
    @Builder.Default
    ValidatorProvider validatorProvider = null;
    @Builder.Default
    PayloadValidator payloadValidator = null;
    @Builder.Default
    Export export = null;
    @Builder.Default
    PlatformTransactionManager platformTransactionManager = null;

}
