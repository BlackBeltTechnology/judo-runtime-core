package hu.blackbelt.judo.runtime.core.dao.rdbms.fixture;

import com.google.common.collect.Maps;
import hu.blackbelt.epsilon.runtime.execution.impl.Slf4jLog;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.framework.compiler.api.CompilerUtil;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.runtime.EsmEpsilonValidator;
import hu.blackbelt.judo.meta.esm.runtime.EsmModel;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilderConfig;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.meta.psm.support.PsmModelResourceSupport;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsUtils;
import hu.blackbelt.judo.meta.script.runtime.ScriptModel;
import hu.blackbelt.judo.meta.script.support.ScriptModelResourceSupport;
import hu.blackbelt.judo.script.codegen.generator.Script2JavaGenerator;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.UUIDIdentifierProvider;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsDAOImpl;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.sequence.RdbmsSequence;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultIdentifierSigner;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultMetricsCollector;
import hu.blackbelt.judo.runtime.core.dispatcher.DispatcherFunctionProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.context.ThreadContext;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.*;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import hu.blackbelt.judo.tatami.asm2expression.Asm2ExpressionConfiguration;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.judo.tatami.core.TransformationTraceServiceImpl;
import hu.blackbelt.judo.tatami.core.workflow.work.WorkReport;
import hu.blackbelt.judo.tatami.core.workflow.work.WorkStatus;
import hu.blackbelt.judo.tatami.script2operation.Script2Operation;
import hu.blackbelt.judo.tatami.workflow.DefaultWorkflowSave;
import hu.blackbelt.judo.tatami.workflow.DefaultWorkflowSetupParameters;
import hu.blackbelt.judo.tatami.workflow.PsmDefaultWorkflow;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import hu.blackbelt.osgi.filestore.security.api.*;
import hu.blackbelt.osgi.filestore.security.api.exceptions.InvalidTokenException;
import hu.blackbelt.osgi.liquibase.StreamResourceAccessor;
import hu.blackbelt.structured.map.proxy.CompositeClassLoader;
import liquibase.Liquibase;
import liquibase.exception.DatabaseException;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.ttddyy.dsproxy.listener.logging.DefaultQueryLogEntryCreator;
import net.ttddyy.dsproxy.listener.logging.SLF4JQueryLoggingListener;
import org.eclipse.emf.common.util.BasicEMap;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static hu.blackbelt.judo.framework.compiler.api.CompilerContext.compilerContextBuilder;
import static hu.blackbelt.judo.meta.esm.runtime.EsmEpsilonValidator.calculateEsmValidationScriptURI;
import static hu.blackbelt.judo.meta.esm.runtime.EsmModel.SaveArguments.esmSaveArgumentsBuilder;
import static hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel.SaveArguments.liquibaseSaveArgumentsBuilder;
import static hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseNamespaceFixUriHandler.fixUriOutputStream;
import static hu.blackbelt.judo.meta.psm.runtime.PsmModel.SaveArguments.psmSaveArgumentsBuilder;
import static hu.blackbelt.judo.meta.script.support.ScriptModelResourceSupport.scriptModelResourceSupportBuilder;
import static hu.blackbelt.judo.tatami.esm2psm.Esm2Psm.Esm2PsmParameter.esm2PsmParameter;
import static hu.blackbelt.judo.tatami.esm2psm.Esm2Psm.executeEsm2PsmTransformation;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class RdbmsDaoFixture {

    public static final DataTypeManager DATA_TYPE_MANAGER = new DataTypeManager(new DefaultCoercer());
    public static final int CHUNK_SIZE = 10;
    public static final long SEQUENCE_START = 20L;
    public static final long SEQUENCE_INCREMENT = 2L;
    public final Context context = new ThreadContext(DATA_TYPE_MANAGER);
    public DefaultVariableResolver variableResolver = new DefaultVariableResolver(DATA_TYPE_MANAGER, context);

    public static final TokenIssuer FILESTORE_TOKEN_ISSUER = new TokenIssuer() {
        @Override
        public String createUploadToken(Token<UploadClaim> token) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String createDownloadToken(Token<DownloadClaim> token) {
            throw new UnsupportedOperationException();
        }
    };

    public static final TokenValidator FILESTORE_TOKEN_VALIDATOR = new TokenValidator() {
        @Override
        public Token<UploadClaim> parseUploadToken(String tokenString) throws InvalidTokenException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Token<DownloadClaim> parseDownloadToken(String tokenString) throws InvalidTokenException {
            throw new UnsupportedOperationException();
        }
    };

    @Getter
    private DefaultDispatcher<UUID> dispatcher;
    private DispatcherFunctionProvider dispatcherFunctionProvider;

    @Getter
    protected AsmModel asmModel;

    @Getter
    protected MeasureModel measureModel;

    @Getter
    protected RdbmsModel rdbmsModel;

    @Getter
    protected LiquibaseModel liquibaseModel;

    @Getter
    protected ExpressionModel expressionModel;

    @Getter
    protected Asm2RdbmsTransformationTrace asm2RdbmsTransformationTrace;

    @Getter
    protected RdbmsResolver rdbmsResolver;

    @Getter
    protected RdbmsUtils rdbmsUtils;

    @Getter
    RdbmsParameterMapper rdbmsParameterMapper;

    @Getter
    protected TransformationTraceService transformationTraceService;

    @Getter
    protected IdentifierProvider<UUID> uuid = new UUIDIdentifierProvider();

    private ByteArrayOutputStream liquibaseStream;

    @Getter
    private ScriptModel scriptModel;

    @Getter
    private String modelName;

    @Getter
    private PsmModel psmModel;

    @Getter
    private AsmUtils asmUtils;

    @Setter
    private boolean ignoreSdk = true;

    @Setter
    private boolean validateModels = Boolean.parseBoolean(System.getProperty("validateModels", "false"));

    private RdbmsDAOImpl<UUID> cachedDao = null;

    private Script2JavaGenerator scriptGenerator = new Script2JavaGenerator();

    private Map<String, Function<Payload, Payload>> operationImplementations = new ConcurrentHashMap<>();

    private boolean initialized;

    @Getter
    private RdbmsDatasourceFixture rdbmsDatasourceFixture;

    private MetricsCollector metricsCollector;

    public RdbmsDaoFixture(String modelName, MetricsCollector metricsCollector) {
        this.modelName = modelName;
        this.metricsCollector = metricsCollector;
        if (metricsCollector instanceof DefaultMetricsCollector) {
            ((DefaultMetricsCollector) metricsCollector).setContext(context);
        }
    }

    public void init(hu.blackbelt.judo.meta.esm.namespace.Model model, RdbmsDatasourceFixture datasourceFixture) {

        try {
            final EsmModel esmModel = createEsmModel(model);

            if (validateModels) {
                if (!esmModel.isValid()) {
                    throw new IllegalStateException("Esm model is invalid:\n" + esmModel.getDiagnosticsAsString());
                }

                EsmEpsilonValidator.validateEsm(new Slf4jLog(log), esmModel, calculateEsmValidationScriptURI());
            }

            esmModel.saveEsmModel(esmSaveArgumentsBuilder()
                    .file(new File("target/test-classes/" + modelName + "-esm.model"))
                    .build());

            java.lang.String createdSourceModelName = "urn:psm.judo-meta-psm";
            PsmModel psmModel = PsmModel.buildPsmModel().uri(URI.createURI(createdSourceModelName)).name("demo").build();

            executeEsm2PsmTransformation(esm2PsmParameter()
                    .esmModel(esmModel)
                    .psmModel(psmModel));

            psmModel.savePsmModel(psmSaveArgumentsBuilder()
                    .file(new File("target/test-classes/" + modelName + "-psm.model"))
                    .build());

            init(PsmModelResourceSupport
                    .psmModelResourceSupportBuilder()
                    .resourceSet(psmModel.getResourceSet()).uri(psmModel.getUri()).build()
                    .getStreamOf(Model.class).findFirst().get(), datasourceFixture);

        } catch (Throwable e) {
            log.error("Exception in RdbmDaoFixture.init: ", e);
//            throw new AssertionFailedError("Error in RdbmsDaoFixture.init - see previous errors: " + e.toString());
//            assertEquals("no error", "error");
//            fail(e);
//            throw new IllegalArgumentException(e);
        }
    }

    public void init(Model model, RdbmsDatasourceFixture datasourceFixture) {

        try {
            this.psmModel = createPsmModel(model);
            this.rdbmsDatasourceFixture = datasourceFixture;

            final PsmDefaultWorkflow defaultWorkflow = new PsmDefaultWorkflow(
                    DefaultWorkflowSetupParameters.defaultWorkflowSetupParameters()
                            .psmModel(psmModel)
                            .modelName(modelName)
                            .ignorePsm2MeasureTrace(true)
                            .ignoreAsm2jaxrsapi(true)
                            .ignoreAsm2Openapi(true)
                            .ignoreAsm2sdk(ignoreSdk)
                            .ignoreAsm2Keycloak(true)
                            .ignoreScript2Operation(true)
                            .validateModels(validateModels)
                            .runInParallel(false)
                            .dialectList(Collections.singletonList(rdbmsDatasourceFixture.getDialect())));
            Asm2ExpressionConfiguration asm2ExpressionConfiguration = new Asm2ExpressionConfiguration();
            // relax validation of lambda variables in tests
            asm2ExpressionConfiguration.setResolveOnlyCurrentLambdaScope(false);
            defaultWorkflow.getTransformationContext().put(asm2ExpressionConfiguration);
            final WorkReport workReport = defaultWorkflow.startDefaultWorkflow();
            assertEquals(WorkStatus.COMPLETED, workReport.getStatus());
            DefaultWorkflowSave.saveModels(defaultWorkflow.getTransformationContext(),
                    new File("target/test-classes/"),
                    Collections.singletonList(rdbmsDatasourceFixture.getDialect()));

            asmModel = defaultWorkflow.getTransformationContext().getByClass(AsmModel.class).get();

            measureModel = defaultWorkflow.getTransformationContext().getByClass(MeasureModel.class).get();
            rdbmsModel = defaultWorkflow.getTransformationContext().get(RdbmsModel.class, "rdbms:" + rdbmsDatasourceFixture.getDialect()).get();
            liquibaseModel = defaultWorkflow.getTransformationContext().get(LiquibaseModel.class, "liquibase:" + rdbmsDatasourceFixture.getDialect()).get();
            expressionModel = defaultWorkflow.getTransformationContext().getByClass(ExpressionModel.class).get();
            scriptModel = defaultWorkflow.getTransformationContext().getByClass(ScriptModel.class).get();

            asm2RdbmsTransformationTrace = defaultWorkflow.getTransformationContext().get(Asm2RdbmsTransformationTrace.class, "asm2rdbmstrace:" + rdbmsDatasourceFixture.getDialect()).get();

            asmUtils = new AsmUtils(asmModel.getResourceSet());

            transformationTraceService = new TransformationTraceServiceImpl();
            transformationTraceService.add(asm2RdbmsTransformationTrace);
            rdbmsResolver = new RdbmsResolver(asmModel, transformationTraceService);
            rdbmsUtils = new RdbmsUtils(rdbmsModel.getResourceSet());
            rdbmsParameterMapper = new RdbmsParameterMapper(DATA_TYPE_MANAGER.getCoercer(), rdbmsModel, getIdProvider(), Dialect.parse(datasourceFixture.getDialect(), datasourceFixture.isJooqEnabled()));

            liquibaseStream = new ByteArrayOutputStream();
            liquibaseModel.saveLiquibaseModel(liquibaseSaveArgumentsBuilder()
                    .outputStream(fixUriOutputStream(liquibaseStream)));

            SLF4JQueryLoggingListener loggingListener = new SLF4JQueryLoggingListener();
            loggingListener.setQueryLogEntryCreator(new DefaultQueryLogEntryCreator());
            final EMap<EOperation, Function<Payload, Payload>> scripts = new BasicEMap<>();
            dispatcherFunctionProvider = new DispatcherFunctionProvider() {
                @Override
                public EMap<EOperation, Function<Payload, Payload>> getSdkFunctions() {
                    return ECollections.emptyEMap();
                }

                @Override
                public EMap<EOperation, Function<Payload, Payload>> getScriptFunctions() {
                    return scripts;
                }
            };
            IdentifierSigner identifierSigner = new DefaultIdentifierSigner<>(asmModel, uuid, DATA_TYPE_MANAGER);
            ActorResolver actorResolver = new DefaultActorResolver<>(DATA_TYPE_MANAGER, getDao(), asmModel, false);
            dispatcher = new DefaultDispatcher<>(asmModel, expressionModel, getDao(), getIdProvider(), dispatcherFunctionProvider, DATA_TYPE_MANAGER, identifierSigner, actorResolver, rdbmsDatasourceFixture.getTransactionManager(), context, metricsCollector, FILESTORE_TOKEN_ISSUER, FILESTORE_TOKEN_VALIDATOR);

            Map<String, String> sourceCodesByFqName = Maps.newHashMap();
            ScriptModelResourceSupport scriptModelResourceSupport =
                    scriptModelResourceSupportBuilder().resourceSet(getScriptModel().getResourceSet()).build();
            StringBuilder sourceCodeText = new StringBuilder();

            CompositeClassLoader compositeClassLoader = new CompositeClassLoader(Script2Operation.class.getClassLoader());

            scriptModelResourceSupport.getStreamOfScriptBindingOperationBinding()
                    .forEach(binding -> {
                        String packageName = scriptGenerator.generatePackageName(binding.getTypeName());
                        String unitName = scriptGenerator.generateClassName(binding.getOperationName());
                        String sourceCode = String.valueOf(scriptGenerator.generate(binding.getScript(), binding));
                        String[] lines = sourceCode.split("\n");
                        for (int i = 0; i < lines.length; i++) {
                            sourceCodeText.append("/*" + (i + 1) + "*/ " + lines[i]);
                            sourceCodeText.append("\n");
                        }
                        sourceCodesByFqName.put(packageName + "." + unitName, sourceCode);
                    });
            log.debug(sourceCodeText.toString());

            // Write to filesystem
            File sourceCodeDirectory = new File("target/test-classes/" + modelName + "/operations");
            if (sourceCodeDirectory.exists()) {
                Files.walk(sourceCodeDirectory.toPath())
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
            sourceCodeDirectory.mkdirs();
            sourceCodesByFqName.entrySet().stream().forEach(e -> {
                File outFile = new File(sourceCodeDirectory, e.getKey().replaceAll("\\.", "/" ) + ".java");
                outFile.getParentFile().mkdirs();
                try {
                    Files.write(outFile.toPath(), e.getValue().getBytes(StandardCharsets.UTF_8));
                } catch (IOException ioException) {
                    throw new RuntimeException("Could not write file: " + outFile.getAbsolutePath(), ioException);
                }
            });

            if (!sourceCodesByFqName.isEmpty()) {
                Iterable<Class> compiled = CompilerUtil.compileAsClass(compilerContextBuilder()
                        .classLoader(compositeClassLoader)
                        .useEclipseCompiler(true)
                        .compilationFiles(listjavaFileTree(sourceCodeDirectory))
// Compilation from inmemory source code
//                        .compilationUnits(
//                                sourceCodesByFqName.entrySet().stream()
//                                        .map(
//                                                e -> JavaFileObjects.forSourceLines(e.getKey(), e.getValue()))
//                                        .collect(Collectors.toList())
//                        )
                        .build());

                scriptModelResourceSupport.getStreamOfScriptBindingOperationBinding()
                        .forEach(binding -> {
                            Function<Payload, Payload> operationImplementation = StreamSupport.stream(compiled.spliterator(), false)
                                    .filter(clazz -> Function.class.isAssignableFrom(clazz))
                                    .filter(clazz -> clazz.getSimpleName().equalsIgnoreCase(binding.getOperationName()))
                                    .map(aClass -> {
                                        try {
                                            return aClass.getDeclaredConstructor().newInstance();
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }).map(instance -> (Function<Payload, Payload>) instance)
                                    .findAny().get();
                            String operationFqName = String.format("%s#%s", binding.getTypeName(), binding.getOperationName()).replaceAll("::", ".");
                            scripts.put(asmUtils.resolveOperation(operationFqName).get(), operationImplementation);
                            operationImplementations.put(binding.getOperationName(), operationImplementation);
                            Class aClass = operationImplementation.getClass();
                            try {
                                aClass.getMethod("setDao", DAO.class).invoke(operationImplementation, getDao());
                                aClass.getMethod("setDispatcher", Dispatcher.class).invoke(operationImplementation, getDispatcher());
                                aClass.getMethod("setIdProvider", IdentifierProvider.class).invoke(operationImplementation, getIdProvider());
                                aClass.getMethod("setAsmModel", AsmModel.class).invoke(operationImplementation, getAsmModel());
                                aClass.getMethod("setVariableResolver", VariableResolver.class).invoke(operationImplementation, variableResolver);
                            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                                throw new RuntimeException(e);
                            }

                        });
            }
            createDatabase();
            metricsCollector.start("TEST");
            this.initialized = true;
        } catch (Exception e) {
            log.error("Exception in RdbmDaoFixture.init: ", e);
//            throw new AssertionFailedError("RdbmDaoFixture.init: " + e.toString(), e);
//            assertEquals("no error", "error");
//            fail(e);
//            throw new RuntimeException(e);
        }
    }

    public void createDatabase() {
        try {
            rdbmsDatasourceFixture.setLiquibaseDbDialect(rdbmsDatasourceFixture.getDataSource().getConnection());
            final Liquibase liquibase = new Liquibase(getLiquibaseName(),
                    new StreamResourceAccessor(Collections.singletonMap(getLiquibaseName(), new ByteArrayInputStream(liquibaseStream.toByteArray()))),
                    rdbmsDatasourceFixture.getLiquibaseDb());
            liquibase.update((String) null);
            rdbmsDatasourceFixture.liquibaseDb.close();
        } catch (Exception e) {
            log.error("CREATE DATABASE", e);
            throw new RuntimeException(e);
        }
    }

    public void dropDatabase() {
        if (rdbmsDatasourceFixture != null && liquibaseStream != null) {
            try {
                rdbmsDatasourceFixture.setLiquibaseDbDialect(rdbmsDatasourceFixture.getDataSource().getConnection());
                final Liquibase liquibase = new Liquibase(getLiquibaseName(),
                        new StreamResourceAccessor(Collections.singletonMap(getLiquibaseName(), new ByteArrayInputStream(liquibaseStream.toByteArray()))),
                        rdbmsDatasourceFixture.getLiquibaseDb());
                liquibase.dropAll();
            } catch (Exception e) {
                log.error("DROP DATABASE", e);
                throw new RuntimeException(e);
            } finally {
                try {
                    rdbmsDatasourceFixture.liquibaseDb.close();
                } catch (DatabaseException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        metricsCollector.stop("TEST");
        context.removeAll();
    }

    private String getLiquibaseName() {
        return modelName + ".changelog.xml";
    }


    public DAO<UUID> getDao() {
        if (cachedDao == null) {
            cachedDao = new RdbmsDAOImpl(DATA_TYPE_MANAGER, asmModel, rdbmsModel, rdbmsDatasourceFixture.getJooqDataSource(), transformationTraceService, new UUIDIdentifierProvider(), rdbmsDatasourceFixture.getDialect(),  rdbmsDatasourceFixture.isJooqEnabled(), true, CHUNK_SIZE, false, variableResolver, context);
            JqlExpressionBuilderConfig expressionBuilderConfig = new JqlExpressionBuilderConfig();
            expressionBuilderConfig.setResolveOnlyCurrentLambdaScope(false);
            cachedDao.setExpressionBuilderConfig(expressionBuilderConfig);
            cachedDao.setMetricsCollector(metricsCollector);
            cachedDao.setMeasureModel(measureModel);
            variableResolver.registerSupplier("SYSTEM", "current_timestamp", new CurrentTimestampProvider(), false);
            variableResolver.registerSupplier("SYSTEM", "current_date", new CurrentDateProvider(), false);
            variableResolver.registerSupplier("SYSTEM", "current_time", new CurrentTimeProvider(), false);
            variableResolver.registerFunction("ENVIRONMENT", new EnvironmentVariableProvider(), true);
            variableResolver.registerFunction("SEQUENCE", new SequenceProvider(new RdbmsSequence(rdbmsDatasourceFixture.getJooqDataSource(), SEQUENCE_START, SEQUENCE_INCREMENT, true, Dialect.parse(rdbmsDatasourceFixture.getDialect(), rdbmsDatasourceFixture.isJooqEnabled()))), false);
        }
        return cachedDao;
    }

    public void addCustomJoinDefinition(final EReference reference, final CustomJoinDefinition customJoinDefinition) {
        cachedDao.getCustomJoinDefinitions().put(reference, customJoinDefinition);
    }

    public IdentifierProvider<UUID> getIdProvider() {
        return uuid;
    }

    public List<Payload> getContents(String fqName, String... references) {
        List<Payload> result = new ArrayList<>();
        EClass classByFQName = getAsmClass(fqName);
        List<Payload> classPayloads = getDao().getAllOf(classByFQName);
        result.addAll(classPayloads);
        for (String referenceName : references) {
            classByFQName.getEAllReferences().stream().filter(ref -> ref.getName().equals(referenceName)).findAny().ifPresent(ref -> {
                for (Payload payload : classPayloads) {
                    List<Payload> navigationResultAt = getDao().getNavigationResultAt((UUID) payload.get(getIdProvider().getName()), ref);
                    result.addAll(navigationResultAt);
                }
            });
        }
        return result;
    }

    public Payload getFromDb(Payload payload) {
        String classFqName = payload.getAs(String.class, "__toType");
        EClass classByFqName = getAsmClass(classFqName);
        UUID uuid = payload.getAs(UUID.class, getIdProvider().getName());
        return getDao().getByIdentifier(classByFqName, uuid).get();

    }

    public EClass getAsmClass(String fqName) {
        return asmUtils.getClassByFQName(fqName).get();
    }

    public Map<String, Function<Payload, Payload>> getOperationImplementations() {
        return Collections.unmodifiableMap(operationImplementations);
    }

    public boolean isInitialized() {
        return initialized;
    }

    public Context getContext() {
        return context;
    }

    private PsmModel createPsmModel(Model model) {
        java.lang.String createdSourceModelName = "urn:psm.judo-meta-psm";
        PsmModel psmModel = PsmModel.buildPsmModel().uri(URI.createURI(createdSourceModelName)).name("demo").build();
        psmModel.addContent(model);
        return psmModel;
    }

    private EsmModel createEsmModel(hu.blackbelt.judo.meta.esm.namespace.Model model) {
        java.lang.String createdSourceModelName = "urn:esm.judo-meta-esm";
        EsmModel esmModel = EsmModel.buildEsmModel().uri(URI.createURI(createdSourceModelName)).name("demo").build();
        esmModel.addContent(model);
        return esmModel;
    }

    @SneakyThrows
    public <R> R runInTransaction(Supplier<R> executable) {
        return rdbmsDatasourceFixture.runInTransaction(executable);
    }

    @SneakyThrows
    public void beginTransaction() {
        getRdbmsDatasourceFixture().getTransactionManager().begin();
    }

    @SneakyThrows
    public void commitTransaction() {
        getRdbmsDatasourceFixture().getTransactionManager().commit();
    }

    @SneakyThrows
    public void rollbackTransaction() {
        getRdbmsDatasourceFixture().getTransactionManager().rollback();
    }

    private static Collection<File> listjavaFileTree(File dir) {
        Set<File> fileTree = new HashSet<File>();
        if(dir == null || dir.listFiles() == null) {
            return fileTree;
        }
        for (File entry : dir.listFiles()) {
            if (entry.isFile() && entry.getAbsolutePath().endsWith(".java")) {
                fileTree.add(entry);
            }
            else if (entry.isDirectory()) {
                fileTree.addAll(listjavaFileTree(entry));
            }
        }
        return fileTree;
    }

}
