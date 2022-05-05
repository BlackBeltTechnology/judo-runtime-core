package hu.blackbelt.judo.services.expression;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.tatami.asm2expression.Asm2Expression;
import hu.blackbelt.judo.tatami.psm2asm.Psm2Asm;
import hu.blackbelt.judo.tatami.psm2measure.Psm2Measure;
import hu.blackbelt.model.northwind.Demo;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.*;

import static hu.blackbelt.judo.meta.asm.runtime.AsmModel.buildAsmModel;
import static hu.blackbelt.judo.meta.expression.runtime.ExpressionModel.buildExpressionModel;
import static hu.blackbelt.judo.meta.measure.runtime.MeasureModel.buildMeasureModel;
import static hu.blackbelt.judo.tatami.asm2expression.Asm2Expression.Asm2ExpressionParameter.asm2ExpressionParameter;
import static hu.blackbelt.judo.tatami.asm2expression.Asm2Expression.executeAsm2Expression;
import static hu.blackbelt.judo.tatami.psm2asm.Psm2Asm.Psm2AsmParameter.psm2AsmParameter;
import static hu.blackbelt.judo.tatami.psm2asm.Psm2Asm.executePsm2AsmTransformation;
import static hu.blackbelt.judo.tatami.psm2measure.Psm2Measure.Psm2MeasureParameter.psm2MeasureParameter;

@Slf4j
@DisplayName("Expression builder tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MappedTransferObjectTypeBindingsCollectorTest {

    public static final String NORTHWIND = "northwind";
    private TransferObjectTypeBindingsCollector transferObjectTypeBindingsCollector;
    private AsmUtils asmUtils;

    @BeforeAll
    void setUp() throws Exception {
        Demo demo = new Demo();
        PsmModel psmModel = demo.fullDemo();

        AsmModel asmModel = buildAsmModel().name(NORTHWIND).build();
        executePsm2AsmTransformation(psm2AsmParameter()
                .psmModel(psmModel)
                .asmModel(asmModel));

        MeasureModel measureModel = buildMeasureModel().name(NORTHWIND).build();
        Psm2Measure.executePsm2MeasureTransformation(psm2MeasureParameter()
                .psmModel(psmModel)
                .measureModel(measureModel));

        ExpressionModel expressionModel = buildExpressionModel().name(NORTHWIND).build();
        executeAsm2Expression(asm2ExpressionParameter()
                .asmModel(asmModel)
                .measureModel(measureModel)
                .expressionModel(expressionModel));

        asmUtils = new AsmUtils(asmModel.getResourceSet());

        transferObjectTypeBindingsCollector = new TransferObjectTypeBindingsCollector(asmModel.getResourceSet(),
                expressionModel.getResourceSet());
    }

    @Test
    @DisplayName("Get transfer object graph")
    void testGetTransferObjectGraph() {
        final EClass shipperInfo = asmUtils.getClassByFQName("demo.services.ShipperInfo").get();
        final EClass orderInfo = asmUtils.getClassByFQName("demo.services.OrderInfo").get();
        final EClass internationalOrderInfo = asmUtils.getClassByFQName("demo.services.InternationalOrderInfo").get();
        final EClass orderItem = asmUtils.getClassByFQName("demo.services.OrderItem").get();

        final MappedTransferObjectTypeBindings shipperInfoGraph = transferObjectTypeBindingsCollector.getTransferObjectGraph(shipperInfo).get();
        log.info("Transfer object graph of ShipperInfo: \n{}", shipperInfoGraph);

        final MappedTransferObjectTypeBindings orderInfoGraph = transferObjectTypeBindingsCollector.getTransferObjectGraph(orderInfo).get();
        log.info("Transfer object graph of OrderInfo: \n{}", orderInfoGraph);

        final MappedTransferObjectTypeBindings internationalOrderInfoGraph = transferObjectTypeBindingsCollector.getTransferObjectGraph(internationalOrderInfo).get();
        log.info("Transfer object graph of InternationalOrderInfo: \n{}", internationalOrderInfoGraph);

        final MappedTransferObjectTypeBindings orderItemGraph = transferObjectTypeBindingsCollector.getTransferObjectGraph(orderItem).get();
        log.info("Transfer object graph of OrderItem: \n{}", orderItemGraph);
    }

    @Test
    @DisplayName("Get exposed graphs")
    void testGetExposedGraphs() {
        final EClass internalAP = asmUtils.getClassByFQName("demo.internalAP").get();
        final EClass externalAP = asmUtils.getClassByFQName("demo.externalAP").get();

        log.info("Exposed graphs of internalAP:\n{}", transferObjectTypeBindingsCollector.getTransferObjectBindings(internalAP).get());
        log.info("Exposed graphs of externalAP:\n{}", transferObjectTypeBindingsCollector.getTransferObjectBindings(externalAP).get());
    }
}
