package hu.blackbelt.judo.runtime.core.bootstrap;

import com.google.inject.Guice;
import com.google.inject.Injector;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import net.jmob.guice.conf.core.BindConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static hu.blackbelt.judo.meta.asm.runtime.AsmModel.buildAsmModel;
import static org.junit.jupiter.api.Assertions.*;

class JudoDefaultModuleTest {
    @BeforeEach
    void init() {

        AsmModel asmModel = AsmModel.buildAsmModel().name("judo").build();
        RdbmsModel rdbmsModel = RdbmsModel.buildRdbmsModel().name("judo").build();
        MeasureModel measureModel = MeasureModel.buildMeasureModel().name("judo").build();
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
                .asm2rdbms(asm2rdbms)
                .build()));

        DAO dao = injector.getInstance(DAO.class);
        Sequence sequence = injector.getInstance(Sequence.class);
        System.out.println("DAO: " + dao);
        System.out.println("Sequance: " + sequence);

    }

    @Test
    void test() {
        assertEquals(true, true);
    }
}