package hu.blackbelt.judo.runtime.core.spring;

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

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilderConfig;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.accessmanager.DefaultAccessManager;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.AncestorNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import hu.blackbelt.judo.runtime.core.dispatcher.*;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.*;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.runtime.core.security.*;
import hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator;
import hu.blackbelt.judo.runtime.core.validator.DefaultValidatorProvider;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.judo.tatami.core.TransformationTraceServiceImpl;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.osgi.filestore.security.api.TokenIssuer;
import hu.blackbelt.osgi.filestore.security.api.TokenValidator;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


@Configuration
public class JudoDefaultSpringConfiguration {

    @Autowired
    public AsmModel asmModel;

    @Autowired
    public Asm2RdbmsTransformationTrace asm2RdbmsTrace;

    @Autowired
    public RdbmsModel rdbmsModel;

    @Autowired
    public ExpressionModel expressionModel;

    @Autowired
    public MeasureModel measureModel;


    @Autowired
    private DataSource dataSource;

    @Autowired
    private TransactionManager transactionManager;


    @Autowired
    private Coercer coercer;

    @Autowired
    private DataTypeManager dataTypeManager;

    @Autowired
    private IdentifierProvider identifierProvider;

    @Autowired
    private MetricsCollector metricsCollector;

    @Autowired
    DispatcherFunctionProvider dispatcherFunctionProvider;

    @Autowired
    private Context context;

    @Autowired
    private Sequence sequence;

    @Autowired
    private RdbmsParameterMapper rdbmsParameterMapper;


    @Bean
    public AccessManager getAccessManager() {
        return DefaultAccessManager.builder()
                .asmModel(asmModel)
                .build();
    }


    @Bean
    public ModifyStatementExecutor getModifyStatementExecutor(
            TransformationTraceService transformationTraceService,
            RdbmsResolver rdbmsResolver
            ) {
        return ModifyStatementExecutor.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .identifierProvider(identifierProvider)
                .transformationTraceService(transformationTraceService)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .coercer(coercer)
                .rdbmsResolver(rdbmsResolver)
                .build();
    }

    @Bean
    public QueryFactory getQueryFactory() {

        JqlExpressionBuilderConfig jqlExpressionBuilderConfig = new JqlExpressionBuilderConfig();
        jqlExpressionBuilderConfig.setResolveOnlyCurrentLambdaScope(false);

        final AsmJqlExtractor asmJqlExtractor = new AsmJqlExtractor(asmModel.getResourceSet(),
                measureModel.getResourceSet(), URI.createURI("expr:" + asmModel.getName()), jqlExpressionBuilderConfig);

        QueryFactory queryFactory = new QueryFactory(
                asmModel.getResourceSet(),
                measureModel.getResourceSet(),
                asmJqlExtractor.extractExpressions(),
                coercer,
                ECollections.asEMap(new ConcurrentHashMap<>()));
                //requireNonNullElse(customJoinDefinitions, ECollections.asEMap(new ConcurrentHashMap<>())));

        return queryFactory;
    }

    @Bean
    public RdbmsBuilder getRdbmsBuilder(RdbmsResolver rdbmsResolver,
            VariableResolver variableResolver,
            MapperFactory mapperFactory,
            Dialect dialect
    ) {
        AsmUtils asm = new AsmUtils(asmModel.getResourceSet());

        return RdbmsBuilder.builder()
                .rdbmsModel(rdbmsModel)
                .ancestorNameFactory(new AncestorNameFactory(asm.all(EClass.class)))
                .rdbmsResolver(rdbmsResolver)
                .parameterMapper(rdbmsParameterMapper)
                .asmUtils(asm)
                .identifierProvider(identifierProvider)
                .coercer(coercer)
                .variableResolver(variableResolver)
                .mapperFactory(mapperFactory)
                .dialect(dialect)
                .build();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public DAO getDAO(
            InstanceCollector instanceCollector,
            SelectStatementExecutor selectStatementExecutor,
            ModifyStatementExecutor modifyStatementExecutor,
            QueryFactory queryFactory
    ) {
        // TODO: Parameters
        Boolean optimisticLockEnabled = true;
        Boolean markSelectedRangeItems = false;

        RdbmsDAOImpl.RdbmsDAOImplBuilder builder =  RdbmsDAOImpl.builder()
                .dataSource(dataSource)
                .context(context)
                .asmModel(asmModel)
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

    @Bean
    public InstanceCollector getInstanceCollector(RdbmsResolver rdbmsResolver
    ) {
        InstanceCollector instanceCollector = RdbmsInstanceCollector.builder()
                .jdbcTemplate(new NamedParameterJdbcTemplate(dataSource))
                .asmModel(asmModel)
                .rdbmsResolver(rdbmsResolver)
                .rdbmsModel(rdbmsModel)
                .coercer(coercer)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .identifierProvider(identifierProvider)
                .build();
        return instanceCollector;
    }

    @Bean
    public RdbmsResolver getRdbmsResolver(
            TransformationTraceService transformationTraceService
    ) {
        return RdbmsResolver.builder()
                .asmModel(asmModel)
                .transformationTraceService(transformationTraceService)
                .build();
    }

    @Bean
    public SelectStatementExecutor getSelectStatementExecutor(
            QueryFactory queryFactory,
            TransformationTraceService transformationTraceService,
            RdbmsBuilder rdbmsBuilder,
            RdbmsResolver rdbmsResolver
    ) {
        // TODO: Map parameter
        Integer chunkSize = 1000;

        return SelectStatementExecutor.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .queryFactory(queryFactory)
                .dataTypeManager(dataTypeManager)
                .identifierProvider(identifierProvider)
                .metricsCollector(metricsCollector)
                .chunkSize(chunkSize)
                .transformationTraceService(transformationTraceService)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .rdbmsBuilder(rdbmsBuilder)
                .queryFactory(queryFactory)
                .rdbmsResolver(rdbmsResolver)
                .build();
    }

    @Bean
    public TransformationTraceService getTransformationTraceService() {
        TransformationTraceService transformationTraceService = new TransformationTraceServiceImpl();
        transformationTraceService.add(asm2RdbmsTrace);
        return transformationTraceService;
    }

    @Bean
    @SuppressWarnings("unchecked")
    public ActorResolver getActorResolver(
            DAO dao
    ) {
        // TODO: Map parameter
        Boolean checkMappedActors = false;
        return DefaultActorResolver.builder()
                .dataTypeManager(dataTypeManager)
                .dao(dao)
                .asmModel(asmModel)
                .checkMappedActors(checkMappedActors)
                .build();
    }

    @Autowired(required = false)
    OpenIdConfigurationProvider openIdConfigurationProvider;

    @Autowired(required = false)
    TokenIssuer filestoreTokenIssuer;

    @Autowired(required = false)
    TokenValidator filestoreTokenValidator;


    @Bean
    public ValidatorProvider validatorProvider(DAO dao) {
        return new DefaultValidatorProvider(dao, identifierProvider, asmModel, context);
    }
    @Bean
    @SuppressWarnings("unchecked")
    public PayloadValidator getPayloadValidator(ValidatorProvider validatorProvider) {
        // TODO: Parameters
        String requiredStringValidatorOption = "ACCEPT_NON_EMPTY";

        AsmUtils asm = new AsmUtils(asmModel.getResourceSet());

        return DefaultPayloadValidator.builder()
                .asmUtils(asm)
                .identifierProvider(identifierProvider)
                .coercer(coercer)
                .validatorProvider(validatorProvider)
                .requiredStringValidatorOption(DefaultPayloadValidator.RequiredStringValidatorOption.valueOf(requiredStringValidatorOption))
                .build();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public Dispatcher getDispatcher(
            DAO dao,
            AccessManager accessManager,
            IdentifierSigner identifierSigner,
            ActorResolver actorResolver,
            PayloadValidator payloadValidator,
            ValidatorProvider validatorProvider
    ) {
        // TODO: Parameters
        Boolean metricsReturned = true;
        Boolean enableValidation = true;
        Boolean trimString = false;
        Boolean caseInsensitiveLike = false;

        return DefaultDispatcher.builder()
                .asmModel(asmModel)
                .expressionModel(expressionModel)
                .dao(dao)
                .identifierProvider(identifierProvider)
                .dispatcherFunctionProvider(dispatcherFunctionProvider)
                .transactionManager(transactionManager)
                .dataTypeManager(dataTypeManager)
                .identifierSigner(identifierSigner)
                .accessManager(accessManager)
                .actorResolver(actorResolver)
                .context(context)
                .metricsCollector(metricsCollector)
                .openIdConfigurationProvider(openIdConfigurationProvider)
                .filestoreTokenValidator(filestoreTokenValidator)
                .filestoreTokenIssuer(filestoreTokenIssuer)
                .payloadValidator(payloadValidator)
                .validatorProvider(validatorProvider)
                .metricsReturned(metricsReturned)
                .enableValidation(enableValidation)
                .trimString(trimString)
                .caseInsensitiveLike(caseInsensitiveLike)
                .build();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public IdentifierSigner getIdentifierSigner() {
        // TODO: Parameter
        String secret = null;

        return DefaultIdentifierSigner.builder()
                .asmModel(asmModel)
                .identifierProvider(identifierProvider)
                .dataTypeManager(dataTypeManager)
                .secret(secret)
                .build();
    }

    @Bean
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public VariableResolver getVariableResolver() {
        DefaultVariableResolver variableResolver = new DefaultVariableResolver(dataTypeManager, context);
        variableResolver.registerSupplier("SYSTEM", "current_timestamp", new CurrentTimestampProvider(), false);
        variableResolver.registerSupplier("SYSTEM", "current_date", new CurrentDateProvider(), false);
        variableResolver.registerSupplier("SYSTEM", "current_time", new CurrentTimeProvider(), false);
        variableResolver.registerFunction("ENVIRONMENT", new EnvironmentVariableProvider(), true);
        variableResolver.registerFunction("SEQUENCE", new SequenceProvider(sequence), false);
        return variableResolver;
    }

}


