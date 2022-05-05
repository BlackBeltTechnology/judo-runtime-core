package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.TwoWayRelationMember;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static hu.blackbelt.judo.dao.api.Payload.empty;
import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newNumericTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class AggregatedFilterTest {
    public static final String MODEL_NAME = "AggregatedFilterTest";

    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected Model getEsmModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(16).withScale(0).build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(MODEL_NAME).build();

        final EntityType orderDetail = newEntityTypeBuilder()
                .withName("OrderDetail")
                .withAttributes(newDataMemberBuilder()
                        .withName("product")
                        .withMemberType(MemberType.STORED)
                        .withDataType(stringType)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("quantity")
                        .withMemberType(MemberType.STORED)
                        .withDataType(integerType)
                        .build())
                .build();
        useEntityType(orderDetail).withMapping(newMappingBuilder().withTarget(orderDetail).build()).build();

        final TwoWayRelationMember customerOfOrder = newTwoWayRelationMemberBuilder()
                .withName("customer")
                .withMemberType(MemberType.STORED)
                .withLower(0).withUpper(1)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();
        final EntityType order = newEntityTypeBuilder()
                .withName("Order")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("items")
                        .withMemberType(MemberType.STORED)
                        .withTarget(orderDetail)
                        .withRelationKind(RelationKind.COMPOSITION)
                        .withLower(1).withUpper(-1)
                        .build())
                .withRelations(customerOfOrder)
                .build();
        useEntityType(order).withMapping(newMappingBuilder().withTarget(order).build()).build();

        final TwoWayRelationMember ordersOfCustomer = newTwoWayRelationMemberBuilder()
                .withName("orders")
                .withMemberType(MemberType.STORED)
                .withTarget(order)
                .withLower(0).withUpper(-1)
                .withRelationKind(RelationKind.AGGREGATION)
                .withPartner(customerOfOrder)
                .build();
        final EntityType customer = newEntityTypeBuilder()
                .withName("Customer")
                .withRelations(ordersOfCustomer)
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("ordersWithMultipleItems")
                        .withMemberType(MemberType.DERIVED)
                        .withTarget(order)
                        .withLower(0).withUpper(-1)
                        .withGetterExpression("self.orders!filter(o | o.items!count() > 1)")
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build())
                .build();
        useEntityType(customer).withMapping(newMappingBuilder().withTarget(customer).build()).build();
        useTwoWayRelationMember(customerOfOrder)
                .withPartner(ordersOfCustomer)
                .withTarget(customer)
                .build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType,
                customer, order, orderDetail
        ));
        return model;
    }

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testAggregatedFilter(RdbmsDaoFixture daoFixture) {
        final EClass customerType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Customer").get();
        final EClass orderType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Order").get();

        final EReference ordersOfCustomer = customerType.getEAllReferences().stream().filter(r -> "orders".equals(r.getName())).findAny().get();
        final EReference ordersWithMultipleItemsOfCustomer = customerType.getEAllReferences().stream().filter(r -> "ordersWithMultipleItems".equals(r.getName())).findAny().get();

        final Payload customer = daoFixture.getDao().create(customerType, empty(), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID customerId = customer.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        final Payload order1 = daoFixture.getDao().create(orderType, map(
                "items", Arrays.asList(map(
                        "product", "P1",
                        "quantity", 2
                ), map(
                        "product", "P2",
                        "quantity", 3
                ))
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID order1Id = order1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        final Payload order2 = daoFixture.getDao().create(orderType, map(
                "items", Arrays.asList(map(
                        "product", "P3",
                        "quantity", 5
                ))
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID order2Id = order2.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        daoFixture.getDao().addReferences(ordersOfCustomer, customerId, Arrays.asList(order1Id, order2Id));

        log.debug("Running query...");

        final List<Payload> result = daoFixture.getDao().getNavigationResultAt(customerId, ordersWithMultipleItemsOfCustomer);

        log.debug("Result: {}", result);

        assertEquals(1, result.size());
    }
}
