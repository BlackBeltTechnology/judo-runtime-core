package hu.blackbelt.judo.runtime.core.bootstrap;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;

import hu.blackbelt.epsilon.runtime.execution.impl.Slf4jLog;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.support.AsmModelResourceSupport;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport;
import hu.blackbelt.judo.meta.liquibase.databaseChangeLog;
import hu.blackbelt.judo.meta.liquibase.impl.databaseChangeLogImpl;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.liquibase.support.LiquibaseModelResourceSupport;
import hu.blackbelt.judo.meta.liquibase.util.builder.databaseChangeLogBuilder;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.measure.support.MeasureModelResourceSupport;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbms.support.RdbmsModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsDataTypes.support.RdbmsDataTypesModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsNameMapping.support.RdbmsNameMappingModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsRules.support.RdbmsTableMappingRulesModelResourceSupport;
import hu.blackbelt.judo.meta.script.runtime.ScriptModel;
import hu.blackbelt.judo.meta.script.support.ScriptModelResourceSupport;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.JudoHsqldbModules;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static hu.blackbelt.judo.tatami.asm2rdbms.ExcelMappingModels2Rdbms.*;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class JudoDefaultModuleTest {

    @Inject
    DAO dao;

    @Inject
    Sequence sequence;

    @Inject
    Dispatcher dispatcher;

    Injector injector;

    @BeforeEach
    void init() throws Exception {
    	    	

        AsmModel asmModel = AsmModel.buildAsmModel()
                .name("judo")
                .resourceSet(AsmModelResourceSupport.createAsmResourceSet())
                .build();

        RdbmsModel rdbmsModel = RdbmsModel.buildRdbmsModel()
                .name("judo")
                .resourceSet(RdbmsModelResourceSupport.createRdbmsResourceSet())
                .build();


        // The RDBMS model resources have to know the mapping models
        RdbmsNameMappingModelResourceSupport.registerRdbmsNameMappingMetamodel(rdbmsModel.getResourceSet());
        RdbmsDataTypesModelResourceSupport.registerRdbmsDataTypesMetamodel(rdbmsModel.getResourceSet());
        RdbmsTableMappingRulesModelResourceSupport.registerRdbmsTableMappingRulesMetamodel(rdbmsModel.getResourceSet());
        injectExcelMappings(rdbmsModel, new Slf4jLog(log), calculateExcelMapping2RdbmsTransformationScriptURI(), calculateExcelMappingModelURI(), "hsqldb");

        MeasureModel measureModel = MeasureModel.buildMeasureModel()
                .name("judo")
                .resourceSet(MeasureModelResourceSupport.createMeasureResourceSet())
                .build();

        ExpressionModel expressionModel = ExpressionModel.buildExpressionModel()
                .name("judo")
                .resourceSet(ExpressionModelResourceSupport.createExpressionResourceSet())
                .build();

        ScriptModel scriptModel = ScriptModel.buildScriptModel()
                .name("judo")
                .resourceSet(ScriptModelResourceSupport.createScriptResourceSet())
                .build();

        LiquibaseModel liquibaseModel = LiquibaseModel.buildLiquibaseModel()
                .name("judo")
                .resourceSet(LiquibaseModelResourceSupport.createLiquibaseResourceSet())
                .build();

        liquibaseModel.getResource().getContents().add(databaseChangeLogBuilder.create().build());

        Asm2RdbmsTransformationTrace asm2rdbms = Asm2RdbmsTransformationTrace.asm2RdbmsTransformationTraceBuilder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .trace(new HashMap<>())
                .build();

        injector = Guice.createInjector(
        		JudoHsqldbModules.builder().build(),
        		new JudoDefaultModule(this, 
        				JudoModelHolder.builder()
			                .asmModel(asmModel)
			                .rdbmsModel(rdbmsModel)
			                .measureModel(measureModel)
			                .expressionModel(expressionModel)
			                .scriptModel(scriptModel)
                            .liquibaseModel(liquibaseModel)
			                .asm2rdbms(asm2rdbms)
			                .build()));

        log.info("DAO: " + dao);
        log.info("Sequence: " + sequence);
        log.info("dispatcher: " + dispatcher);
    }

    @AfterEach
    void tearDown() {
    }
    
    
    @Test
    void test() {
        assertTrue(true);
    }
}