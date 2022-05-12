package hu.blackbelt.judo.services.query;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.meta.query.Select;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.meta.query.Target;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.tatami.core.workflow.work.WorkReport;
import hu.blackbelt.judo.tatami.core.workflow.work.WorkStatus;
import hu.blackbelt.judo.tatami.workflow.DefaultWorkflowSetupParameters;
import hu.blackbelt.judo.tatami.workflow.PsmDefaultWorkflow;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import hu.blackbelt.model.northwind.Demo;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
public class QueryFactoryTest {

    private QueryFactory queryFactory;
    private AsmUtils asmUtils;

    @BeforeEach
    public void setUp() throws Exception {

        final PsmModel psmModel = new Demo().fullDemo();

        final PsmDefaultWorkflow defaultWorkflow = new PsmDefaultWorkflow(
                    DefaultWorkflowSetupParameters.defaultWorkflowSetupParameters()
                        .psmModel(psmModel)
                        .modelName("northwind")
                        .ignoreAsm2Rdbms(true)
                        .ignoreAsm2sdk(true)
                        .ignoreAsm2jaxrsapi(true)
                        .ignoreAsm2Openapi(true)
                        .ignoreAsm2Keycloak(true)
                        .ignoreAsm2Script(true)
                        .dialectList(Collections.singletonList("hsqldb")));
        final WorkReport workReport = defaultWorkflow.startDefaultWorkflow();
        assertEquals(WorkStatus.COMPLETED, workReport.getStatus());

        AsmModel asmModel = defaultWorkflow.getTransformationContext().getByClass(AsmModel.class).get();
        asmModel.saveAsmModel(AsmModel.SaveArguments.asmSaveArgumentsBuilder()
                .file(new File("target/test-classes/northwind-asm.model"))
                .build());

        MeasureModel measureModel = defaultWorkflow.getTransformationContext().getByClass(MeasureModel.class).get();
        measureModel.saveMeasureModel(MeasureModel.SaveArguments.measureSaveArgumentsBuilder()
                .file(new File("target/test-classes/northwind-measure.model"))
                .build());

        asmUtils = new AsmUtils(asmModel.getResourceSet());

        final ExpressionModel expressionModel = ExpressionModel.buildExpressionModel()
                .name("northwind")
                .uri(URI.createURI("expression:northwind"))
                .build();
        final AsmJqlExtractor extractor = new AsmJqlExtractor(asmModel.getResourceSet(), measureModel.getResourceSet(), expressionModel.getResourceSet());
        extractor.extractExpressions();
        expressionModel.saveExpressionModel(ExpressionModel.SaveArguments.expressionSaveArgumentsBuilder()
                .file(new File("target/test-classes/northwind-expression.model"))
                .build());

        queryFactory = new QueryFactory(asmModel.getResourceSet(), measureModel.getResourceSet(), expressionModel.getResourceSet(), new DefaultCoercer(), ECollections.emptyEMap());
        asmUtils.getClassByFQName("demo.entities.OrderDetail").get();
    }

    @AfterEach
    public void tearDown() {
        queryFactory = null;
        asmUtils = null;
    }

    @Test
    public void testGetQuery() {
        final EClass transferObjectType = asmUtils.getClassByFQName("demo.services.InternationalOrderInfo").get();
        final EReference itemsReference = transferObjectType.getEAllReferences().stream().filter(r -> "items".equals(r.getName())).findAny().get();
        final EReference productReference = itemsReference.getEReferenceType().getEAllReferences().stream().filter(r -> "product".equals(r.getName())).findAny().get();
        final Select select = queryFactory.getQuery(transferObjectType).get();
        if (log.isDebugEnabled()) {
            log.debug("QUERY:\n{}", select);
        }

        final Target targetOfItems = select.getMainTarget().getReferencedTargets().stream().filter(rt -> AsmUtils.equals(rt.getReference(), itemsReference)).map(rt -> rt.getTarget()).findAny().get();
        final Target targetOfProduct = targetOfItems.getReferencedTargets().stream().filter(rt -> AsmUtils.equals(rt.getReference(), productReference)).map(rt -> rt.getTarget()).findAny().get();

        assertNotNull(targetOfItems, "Target of items must present");
        assertNotNull(targetOfProduct, "Target of product must present");

        assertThat("Target of items must be OrderItem", targetOfItems.getType(), equalTo(asmUtils.getClassByFQName("demo.services.OrderItem").get()));
        assertThat("Target of product must be ProductOrder", targetOfProduct.getType(), equalTo(asmUtils.getClassByFQName("demo.services.ProductInfo").get()));

        final EClass internalAP = asmUtils.getClassByFQName("demo.InternalUser").get();
        final EReference ordersOfLastTwoWeek = internalAP.getEAllReferences().stream().filter(r -> "LastTwoWeekOrders".equals(r.getName())).findAny().get();

        final SubSelect ordersOfLastTwoWeekSelect = queryFactory.getNavigation(ordersOfLastTwoWeek).get();
        if (log.isDebugEnabled()) {
            log.debug("QUERY:\n{}", ordersOfLastTwoWeekSelect);
        }
    }
}
