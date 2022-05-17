package hu.blackbelt.judo.runtime.core.dao.rdbms.fixture;

import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;

import hu.blackbelt.epsilon.runtime.execution.impl.Slf4jLog;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.framework.compiler.api.CompilerUtil;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.runtime.EsmEpsilonValidator;
import hu.blackbelt.judo.meta.esm.runtime.EsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.meta.psm.support.PsmModelResourceSupport;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.script.runtime.ScriptModel;
import hu.blackbelt.judo.meta.script.support.ScriptModelResourceSupport;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.StreamResourceAccessor;
import hu.blackbelt.judo.runtime.core.dispatcher.DispatcherFunctionProvider;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.script.codegen.generator.Script2JavaGenerator;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoDefaultModule;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelHolder;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.JudoHsqldbDatasourceWrapperModule;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.JudoHsqldbModules;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql.JudoPostgresqlDatasourceWrapperModule;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql.JudoPostgresqlModules;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import hu.blackbelt.judo.tatami.asm2expression.Asm2ExpressionConfiguration;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import hu.blackbelt.judo.tatami.core.workflow.work.WorkReport;
import hu.blackbelt.judo.tatami.core.workflow.work.WorkStatus;
import hu.blackbelt.judo.tatami.script2operation.Script2Operation;
import hu.blackbelt.judo.tatami.workflow.DefaultWorkflowSave;
import hu.blackbelt.judo.tatami.workflow.DefaultWorkflowSetupParameters;
import hu.blackbelt.judo.tatami.workflow.PsmDefaultWorkflow;
import hu.blackbelt.structured.map.proxy.CompositeClassLoader;
import liquibase.Liquibase;
import liquibase.exception.DatabaseException;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.BasicEMap;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;
import org.slf4j.bridge.SLF4JBridgeHandler;
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J;

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
public class JudoRuntimeFixture {

    static {
        SLF4JBridgeHandler.install();
        SysOutOverSLF4J.sendSystemOutAndErrToSLF4J();
    }
    private ByteArrayOutputStream liquibaseStream;
    private Script2JavaGenerator scriptGenerator = new Script2JavaGenerator();
    private Map<String, Function<Payload, Payload>> operationImplementations = new ConcurrentHashMap<>();
    private boolean initialized;
    private Module databaseModule;
    private MeasureModel measureModel;
    private RdbmsModel rdbmsModel;
    private LiquibaseModel liquibaseModel;
    private ExpressionModel expressionModel;
    private Asm2RdbmsTransformationTrace asm2RdbmsTransformationTrace;
    private ScriptModel scriptModel;

    @Getter
    private Injector injector;

    @Getter
    protected AsmModel asmModel;

    @Getter
    private AsmUtils asmUtils;

    @Getter
    private String modelName;

    @Getter
    private PsmModel psmModel;

    @Setter
    private boolean ignoreSdk = true;

    @Setter
    private boolean validateModels = Boolean.parseBoolean(System.getProperty("validateModels", "false"));

    @Setter
    private boolean saveAndReloadModels = Boolean.parseBoolean(System.getProperty("saveAndReloadModels", "true"));

    @Getter
    private JudoDatasourceFixture rdbmsDatasourceFixture;

    @Getter
    private Dialect dialect;
        
    @Inject
    DAO dao;
    
    @Inject
    @Getter
    MetricsCollector metricsCollector;

    @Inject
    @Getter
    InstanceCollector instanceCollector;

    @Inject
    @Getter
    Dispatcher dispatcher;
    
    @Inject
    @Getter
    VariableResolver variableResolver;

    @Inject
    IdentifierProvider idProvider;

    @Inject
    @Getter
    Context context;

    @Inject
    @Getter
    QueryFactory queryFactory;

    @Inject
    @Getter
    DataTypeManager dataTypeManager;

    @Inject
    @Getter
    Sequence sequence;

    @Inject
    @Getter
    RdbmsResolver rdbmsResolver;

    @Inject
    @Getter
    RdbmsParameterMapper rdbmsParameterMapper;

    @Inject
    @Getter
    DispatcherFunctionProvider dispatcherFunctionProvider;

    public JudoRuntimeFixture(String modelName) {
        this.modelName = modelName.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");
    }

    public void init(hu.blackbelt.judo.meta.esm.namespace.Model model, JudoDatasourceFixture datasourceFixture) {
    	
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

    public void init(Model model, JudoDatasourceFixture datasourceFixture) {

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
            asm2ExpressionConfiguration.setResolveOnlyCurrentLambdaScope(false);
            defaultWorkflow.getTransformationContext().put(asm2ExpressionConfiguration);

            final WorkReport workReport = defaultWorkflow.startDefaultWorkflow();
            assertEquals(WorkStatus.COMPLETED, workReport.getStatus());

            File modelDirectory = new File("target/test-classes/");

            DefaultWorkflowSave.saveModels(defaultWorkflow.getTransformationContext(),
                    modelDirectory,
                    Collections.singletonList(rdbmsDatasourceFixture.getDialect()));

            if (rdbmsDatasourceFixture.getDialect().equals("hsqldb")) {
                dialect = new HsqldbDialect();
            	databaseModule = Modules
            			.override(JudoHsqldbModules.builder().build())
            			.with(new JudoHsqldbDatasourceWrapperModule(rdbmsDatasourceFixture.getWrappedDataSource(), rdbmsDatasourceFixture.getTransactionManager()));
            } else if (rdbmsDatasourceFixture.getDialect().equals("postgresql")) {
                dialect = new HsqldbDialect();
            	databaseModule = Modules
            			.override(JudoPostgresqlModules.builder().build())
            			.with(new JudoPostgresqlDatasourceWrapperModule(rdbmsDatasourceFixture.getWrappedDataSource(), rdbmsDatasourceFixture.getTransactionManager()));
            } else {
                throw new IllegalArgumentException("Unknown dialect: " + rdbmsDatasourceFixture.getDialect());
            }

            JudoModelHolder judoModelHolder;

            if (!saveAndReloadModels) {
                asmModel = defaultWorkflow.getTransformationContext().getByClass(AsmModel.class).get();
                measureModel = defaultWorkflow.getTransformationContext().getByClass(MeasureModel.class).get();
                rdbmsModel = defaultWorkflow.getTransformationContext().get(RdbmsModel.class, "rdbms:" + rdbmsDatasourceFixture.getDialect()).get();
                liquibaseModel = defaultWorkflow.getTransformationContext().get(LiquibaseModel.class, "liquibase:" + rdbmsDatasourceFixture.getDialect()).get();
                expressionModel = defaultWorkflow.getTransformationContext().getByClass(ExpressionModel.class).get();
                scriptModel = defaultWorkflow.getTransformationContext().getByClass(ScriptModel.class).get();
                asm2RdbmsTransformationTrace = defaultWorkflow.getTransformationContext().get(Asm2RdbmsTransformationTrace.class, "asm2rdbmstrace:" + rdbmsDatasourceFixture.getDialect()).get();
                judoModelHolder = JudoModelHolder.builder()
                        .asmModel(asmModel)
                        .rdbmsModel(rdbmsModel)
                        .measureModel(measureModel)
                        .expressionModel(expressionModel)
                        .scriptModel(scriptModel)
                        .asm2rdbms(asm2RdbmsTransformationTrace)
                        .build();

            } else {
                judoModelHolder =  JudoModelHolder.loadFromURL(modelName, modelDirectory.toURI(), dialect, false);
                asmModel = judoModelHolder.getAsmModel();
                measureModel = judoModelHolder.getMeasureModel();
                rdbmsModel = judoModelHolder.getRdbmsModel();
                liquibaseModel = judoModelHolder.getLiquibaseModel();
                expressionModel = judoModelHolder.getExpressionModel();
                scriptModel = judoModelHolder.getScriptModel();
                asm2RdbmsTransformationTrace = judoModelHolder.getAsm2rdbms();
            }

            this.asmUtils = new AsmUtils(asmModel.getResourceSet());

            injector = Guice.createInjector(
            		databaseModule,
            		new JudoDefaultModule(this,
                            judoModelHolder
                            /*
            				JudoModelHolder.builder()
    			                .asmModel(asmModel)
    			                .rdbmsModel(rdbmsModel)
    			                .measureModel(measureModel)
    			                .expressionModel(expressionModel)
    			                .scriptModel(scriptModel)
    			                .asm2rdbms(asm2RdbmsTransformationTrace)
    			                .build() */));

            liquibaseStream = new ByteArrayOutputStream();
            liquibaseModel.saveLiquibaseModel(liquibaseSaveArgumentsBuilder()
                    .outputStream(fixUriOutputStream(liquibaseStream)));

//            SLF4JQueryLoggingListener loggingListener = new SLF4JQueryLoggingListener();
//            loggingListener.setQueryLogEntryCreator(new DefaultQueryLogEntryCreator());
            final EMap<EOperation, Function<Payload, Payload>> scripts = new BasicEMap<>();

            Map<String, String> sourceCodesByFqName = Maps.newHashMap();
            ScriptModelResourceSupport scriptModelResourceSupport =
                    scriptModelResourceSupportBuilder().resourceSet(scriptModel.getResourceSet()).build();
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
                                aClass.getMethod("setAsmModel", AsmModel.class).invoke(operationImplementation, asmModel);
                                aClass.getMethod("setVariableResolver", VariableResolver.class).invoke(operationImplementation, getVariableResolver());
                            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                                throw new RuntimeException(e);
                            }

                        });
            }
            getDispatcherFunctionProvider().getScriptFunctions().putAll(scripts);
            createDatabase();
//            metricsCollector.start("TEST");
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
            rdbmsDatasourceFixture.setLiquibaseDbDialect(rdbmsDatasourceFixture.getOriginalDataSource().getConnection());
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
                rdbmsDatasourceFixture.setLiquibaseDbDialect(rdbmsDatasourceFixture.getOriginalDataSource().getConnection());
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
//        metricsCollector.stop("TEST");
//        context.removeAll();
    }

    private String getLiquibaseName() {
        return modelName + ".changelog.xml";
    }

    public void addCustomJoinDefinition(final EReference reference, final CustomJoinDefinition customJoinDefinition) {
        getQueryFactory().getCustomJoinDefinitions().put(reference, customJoinDefinition);
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
        return (Payload) getDao().getByIdentifier(classByFqName, uuid).get();

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

    @SuppressWarnings("unchecked")
	public IdentifierProvider<UUID> getIdProvider() {
    	return idProvider;
    }

    @SuppressWarnings("unchecked")
	public DAO<UUID> getDao() {
    	return dao;
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
