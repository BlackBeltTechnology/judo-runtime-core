package processors;

import com.google.common.collect.ImmutableList;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.services.core.UUIDIdentifierProvider;
import hu.blackbelt.judo.services.dao.core.collectors.EmptyMapIntstanceCollector;
import hu.blackbelt.judo.services.dao.core.processors.InsertPayloadDaoProcessor;
import hu.blackbelt.judo.services.dao.core.statements.InsertStatement;
import hu.blackbelt.judo.services.dao.core.statements.Statement;
import hu.blackbelt.judo.services.dao.core.values.Metadata;
import hu.blackbelt.judo.services.query.QueryFactory;
import hu.blackbelt.judo.tatami.core.workflow.work.WorkReport;
import hu.blackbelt.judo.tatami.core.workflow.work.WorkStatus;
import hu.blackbelt.judo.tatami.workflow.DefaultWorkflowSetupParameters;
import hu.blackbelt.judo.tatami.workflow.PsmDefaultWorkflow;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import hu.blackbelt.model.northwind.Demo;
import hu.blackbelt.structured.map.proxy.MapHolder;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import sdk.demo.services.CategoryInfo;
import sdk.demo.services.OrderInfo;
import sdk.demo.services.OrderItem;
import sdk.demo.services.ProductInfo;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static hu.blackbelt.judo.dao.api.Payload.asPayload;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class InsertPayloadDaoProcessorTest {

    InsertPayloadDaoProcessor insertPayloadProcessor;

    private AsmModel asmModel;
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
                        .dialectList(Collections.singletonList("hsqldb")));
        final WorkReport workReport = defaultWorkflow.startDefaultWorkflow();
        assertEquals(WorkStatus.COMPLETED, workReport.getStatus());

        asmModel = defaultWorkflow.getTransformationContext().getByClass(AsmModel.class).get();

        asmUtils = new AsmUtils(asmModel.getResourceSet());

        insertPayloadProcessor = new InsertPayloadDaoProcessor(asmModel.getResourceSet(),
                new UUIDIdentifierProvider(),
                new QueryFactory(asmModel.getResourceSet(), new ResourceSetImpl(), new DefaultCoercer()),
                new EmptyMapIntstanceCollector(),
                (clazz) -> Payload.empty(),
                Metadata.buildMetadata()
                        .timestamp(OffsetDateTime.now())
                        .build());
    }

    @Test
    public void testOK() {

        Optional<EClass> orderInfoClass = asmUtils.getClassByFQName("demo.services.OrderInfo");

        OrderInfo orderInfo = OrderInfo.builder()
                .withItems(ImmutableList.of(
                        OrderItem.builder()
                                .withProduct(ProductInfo.builder()
                                        //.identifier(UUID.randomUUID())
                                        .withProductName("Product")
                                        .withUnitPrice(Double.valueOf(10.0))
                                        .withCategory(CategoryInfo.builder().withCategoryName("ddd").build())
                                        .build())
                                .withUnitPrice(Double.valueOf(12.0))
                                .withQuantity(11)
                                .withDiscount(Double.valueOf(0))
                                .build()
                        )
                )
                .withOrderDate(OffsetDateTime.now())
                .build();

        Collection<Statement<UUID>> statements = insertPayloadProcessor.insert(orderInfoClass.get(), asPayload(((MapHolder) orderInfo).toMap()), true);

        assertThat("Statements must not be empty", statements, hasSize(7));

        assertThat("", statements,
                hasItems(
                        allOf(
                                instanceOf(InsertStatement.class),
                                hasProperty("instance",
                                        hasProperty("type",
                                                hasProperty("name", equalTo("Order"))))
                        ),
                        allOf(
                                instanceOf(InsertStatement.class),
                                hasProperty("instance",
                                        hasProperty("type",
                                                hasProperty("name", equalTo("OrderDetail"))))
                        ),
                        allOf(
                                instanceOf(InsertStatement.class),
                                hasProperty("instance",
                                        hasProperty("type",
                                                hasProperty("name", equalTo("Category"))))
                        ),
                        allOf(
                                instanceOf(InsertStatement.class),
                                hasProperty("instance",
                                        hasProperty("type",
                                                hasProperty("name", equalTo("Product"))))
                        )
                )
        );

    }
}
