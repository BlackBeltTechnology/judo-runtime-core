package hu.blackbelt.judo.runtime.core.bootstrap;

import com.google.inject.Guice;
import com.google.inject.Injector;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.support.AsmModelResourceSupport;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.measure.support.MeasureModelResourceSupport;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbms.support.RdbmsModelResourceSupport;
import hu.blackbelt.judo.meta.script.runtime.ScriptModel;
import hu.blackbelt.judo.meta.script.support.ScriptModelResourceSupport;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class JudoDefaultModuleTest {
    @BeforeEach
    void init() {

        AsmModel asmModel = AsmModel.buildAsmModel()
                .name("judo")
                .resourceSet(AsmModelResourceSupport.createAsmResourceSet())
                .build();

        RdbmsModel rdbmsModel = RdbmsModel.buildRdbmsModel()
                .name("judo")
                .resourceSet(RdbmsModelResourceSupport.createRdbmsResourceSet())
                .build();

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

        Asm2RdbmsTransformationTrace asm2rdbms = Asm2RdbmsTransformationTrace.asm2RdbmsTransformationTraceBuilder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .trace(new HashMap<>())
                .build();

        // Asm2RdbmsTransformationTrace asm2RdbmsTransformationTraceLoaded =
        //                fromModelsAndTrace(NORTHWIND, asmModel, rdbmsModel, new File(TARGET_TEST_CLASSES, NORTHWIND_ASM_2_RDBMS_MODEL));
        Injector injector = Guice.createInjector(new JudoDefaultModule(this, JudoModelSpecification.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .measureModel(measureModel)
                .expressionModel(expressionModel)
                .scriptModel(scriptModel)
                .asm2rdbms(asm2rdbms)
                .build()));

        DAO dao = injector.getInstance(DAO.class);
        Sequence sequence = injector.getInstance(Sequence.class);
        Dispatcher dispatcher = injector.getInstance(Dispatcher.class);
        log.info("DAO: " + dao);
        log.info("Sequence: " + sequence);
        log.info("dispatcher: " + dispatcher);

    }

    @Test
    void test() {
        assertEquals(true, true);
    }
}