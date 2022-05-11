package hu.blackbelt.judo.runtime.core.dao.rdbms.script;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newNumericTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class StatisticalTest {

    private static final String FQNAME = "demo._default_transferobjecttypes.entities.";

    public static Payload run(RdbmsDaoFixture fixture, String operationName, Payload exchange) {
        Function<Payload, Payload> operationImplementation =
                operationName != null ? fixture.getOperationImplementations().get(operationName) :
                        fixture.getOperationImplementations().values().iterator().next();
        Payload result;
        try {
            result = operationImplementation.apply(exchange);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static Payload run(RdbmsDaoFixture fixture) {
        return run(fixture, null, null);
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    void testAggregationWithBothNullToInfiniteCardinality(RdbmsDaoFixture testFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();

        modelBuilder.addEntity("Aggregated")
                .withAttribute("Double", "number")
                .withAttribute("Integer", "integer");

        modelBuilder.addEntity("Aggregator")
                .withAggregation("Aggregated", "aggregation", cardinality(0, -1))
                .withProperty("Double", "min", "self.aggregation!min(m | m.number)")
                .withProperty("Double", "max", "self.aggregation!max(m | m.number)")
                .withProperty("Double", "avg", "self.aggregation!avg(a | a.number)")
                .withProperty("Double", "intAvg", "self.aggregation!avg(a | a.integer + 0.0000)")
                .withProperty("Double", "sum", "self.aggregation!sum(m | m.number)")
                .withProperty("Integer", "count", "self.aggregation!count(c)");

        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Aggregator TCr = new demo::entities::Aggregator(" +
                        "new demo::entities::Aggregated[]{" +
                        "new demo::entities::Aggregated(number = 5, integer = 3), " +
                        "new demo::entities::Aggregated(number = 55, integer = 4), " +
                        "new demo::entities::Aggregated(number = 45, integer = 9)});");

        testFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(testFixture.isInitialized(), "DAO initialized");

        run(testFixture);

        assertEquals(5.0, testFixture.getContents(FQNAME + "Aggregator").get(0).get("min"));
        assertEquals(55.0, testFixture.getContents(FQNAME + "Aggregator").get(0).get("max"));
        assertEquals(35.0, testFixture.getContents(FQNAME + "Aggregator").get(0).get("avg"));
        assertEquals(5.3333, testFixture.getContents(FQNAME + "Aggregator").get(0).get("intAvg"));
        assertEquals(105.0, testFixture.getContents(FQNAME + "Aggregator").get(0).get("sum"));
        assertEquals(3, testFixture.getContents(FQNAME + "Aggregator").get(0).get("count"));
    }

    @Test
    void testContainmentWithNullToInfiniteCardinality(RdbmsDaoFixture testFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();

        modelBuilder.addEntity("Contained")
                .withAttribute("Double", "number");

        modelBuilder.addEntity("Container")
                .withContainment("Contained", "containment", cardinality(0, -1))
                .withProperty("Double", "min", "self.containment!min(m | m.number)")
                .withProperty("Double", "max", "self.containment!max(m | m.number)")
                .withProperty("Double", "avg", "self.containment!avg(a | a.number)")
                .withProperty("Double", "sum", "self.containment!sum(m | m.number)")
                .withProperty("Integer", "count", "self.containment!count(c)");

        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Container TCr = new demo::entities::Container(" +
                        "new demo::entities::Contained[]{" +
                        "new demo::entities::Contained(5), " +
                        "new demo::entities::Contained(55), " +
                        "new demo::entities::Contained(45)});");

        testFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(testFixture.isInitialized(), "DAO initialized");

        run(testFixture);

        assertEquals(5.0, testFixture.getContents(FQNAME + "Container").get(0).get("min"));
        assertEquals(55.0, testFixture.getContents(FQNAME + "Container").get(0).get("max"));
        assertEquals(35.0, testFixture.getContents(FQNAME + "Container").get(0).get("avg"));
        assertEquals(105.0, testFixture.getContents(FQNAME + "Container").get(0).get("sum"));
        assertEquals(3, testFixture.getContents(FQNAME + "Container").get(0).get("count"));
    }

    @Test
    void testOneDirectionalAssociationWithNullToInfiniteCardinality(RdbmsDaoFixture testFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();

        modelBuilder.addEntity("Associated")
                .withAttribute("Double", "number");

        modelBuilder.addEntity("Associate")
                .withRelation("Associated", "oneDirectionalAssociation", cardinality(0, -1))
                .withProperty("Double", "min", "self.oneDirectionalAssociation!min(m | m.number)")
                .withProperty("Double", "max", "self.oneDirectionalAssociation!max(m | m.number)")
                .withProperty("Double", "avg", "self.oneDirectionalAssociation!avg(a | a.number)")
                .withProperty("Double", "sum", "self.oneDirectionalAssociation!sum(m | m.number)")
                .withProperty("Integer", "count", "self.oneDirectionalAssociation!count(c)");

        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Associate A = new demo::entities::Associate();\n" +
                        "A.oneDirectionalAssociation += new demo::entities::Associated(number = 684);\n" +
                        "A.oneDirectionalAssociation += new demo::entities::Associated(number = 354);\n" +
                        "A.oneDirectionalAssociation += new demo::entities::Associated(number = 48);\n"
        );

        testFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(testFixture.isInitialized(), "DAO initialized");

        run(testFixture);

        assertEquals(48.0, testFixture.getContents(FQNAME + "Associate").get(0).get("min"));
        assertEquals(684.0, testFixture.getContents(FQNAME + "Associate").get(0).get("max"));
        assertEquals(362.0, testFixture.getContents(FQNAME + "Associate").get(0).get("avg"));
        assertEquals(1086.0, testFixture.getContents(FQNAME + "Associate").get(0).get("sum"));
        assertEquals(3, testFixture.getContents(FQNAME + "Associate").get(0).get("count"));
    }

    @Test
    void testTwoDirectionalAssociationWithBothNullToInfiniteCardinality(RdbmsDaoFixture testFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();

        modelBuilder.addEntity("Associate1")
                .withAttribute("Double", "number")

                .withProperty("Double", "min", "self.association1!min(m | m.number)")
                .withProperty("Double", "max", "self.association1!max(m | m.number)")
                .withProperty("Double", "avg", "self.association1!avg(a | a.number)")
                .withProperty("Double", "sum", "self.association1!sum(m | m.number)")
                .withProperty("Integer", "count", "self.association1!count(c)")

                .withRelation("Associate2", "association1", cardinality(0, -1));

        modelBuilder.addEntity("Associate2")
                .withAttribute("Double", "number")

                .withProperty("Double", "min", "self.association2!min(m | m.number)")
                .withProperty("Double", "max", "self.association2!max(m | m.number)")
                .withProperty("Double", "avg", "self.association2!avg(a | a.number)")
                .withProperty("Double", "sum", "self.association2!sum(m | m.number)")
                .withProperty("Integer", "count", "self.association2!count(c)")

                .withRelation("Associate1", "association2", cardinality(0, -1));

        modelBuilder.setTwoWayRelation("Associate1", "association1", "Associate2", "association2");

        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Associate1 a1 = new demo::entities::Associate1(number = 5);\n" +
                        "var demo::entities::Associate1 b1 = new demo::entities::Associate1(number = 10);\n" +
                        "var demo::entities::Associate1 c1 = new demo::entities::Associate1(number = 15);\n" +

                        "var demo::entities::Associate2 a2 = new demo::entities::Associate2(number = 20);\n" +
                        "var demo::entities::Associate2 b2 = new demo::entities::Associate2(number = 25);\n" +
                        "var demo::entities::Associate2 c2 = new demo::entities::Associate2(number = 30);\n" +

                        "a1.association1 += a2;\n" +
                        "a1.association1 += b2;\n" +
                        "a1.association1 += c2;\n" +

                        "b1.association1 += a2;\n" +
                        "b1.association1 += b2;\n" +
                        "b1.association1 += c2;\n" +

                        "c1.association1 += a2;\n" +
                        "c1.association1 += b2;\n" +
                        "c1.association1 += c2;\n" +

                        "a2.association2 += a1;\n" +
                        "a2.association2 += b1;\n" +
                        "a2.association2 += c1;\n" +

                        "b2.association2 += a1;\n" +
                        "b2.association2 += b1;\n" +
                        "b2.association2 += c1;\n" +

                        "c2.association2 += a1;\n" +
                        "c2.association2 += b1;\n" +
                        "c2.association2 += c1;"
        );

        testFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(testFixture.isInitialized(), "DAO initialized");

        run(testFixture);

        assertEquals(20.0, testFixture.getContents(FQNAME + "Associate1").get(0).get("min")); //a1
        assertEquals(30.0, testFixture.getContents(FQNAME + "Associate1").get(0).get("max")); //a1
        assertEquals(25.0, testFixture.getContents(FQNAME + "Associate1").get(0).get("avg")); //a1
        assertEquals(75.0, testFixture.getContents(FQNAME + "Associate1").get(0).get("sum")); //a1
        assertEquals(3, testFixture.getContents(FQNAME + "Associate1").get(0).get("count")); //a1

        assertEquals(20.0, testFixture.getContents(FQNAME + "Associate1").get(1).get("min")); //b1
        assertEquals(30.0, testFixture.getContents(FQNAME + "Associate1").get(1).get("max")); //b1
        assertEquals(25.0, testFixture.getContents(FQNAME + "Associate1").get(1).get("avg")); //b1
        assertEquals(75.0, testFixture.getContents(FQNAME + "Associate1").get(1).get("sum")); //b1
        assertEquals(3, testFixture.getContents(FQNAME + "Associate1").get(1).get("count")); //b1

        assertEquals(20.0, testFixture.getContents(FQNAME + "Associate1").get(2).get("min")); //c1
        assertEquals(30.0, testFixture.getContents(FQNAME + "Associate1").get(2).get("max")); //c1
        assertEquals(25.0, testFixture.getContents(FQNAME + "Associate1").get(2).get("avg")); //c1
        assertEquals(75.0, testFixture.getContents(FQNAME + "Associate1").get(2).get("sum")); //c1
        assertEquals(3, testFixture.getContents(FQNAME + "Associate1").get(2).get("count")); //c1

        assertEquals(5.0, testFixture.getContents(FQNAME + "Associate2").get(0).get("min")); //a2
        assertEquals(15.0, testFixture.getContents(FQNAME + "Associate2").get(0).get("max")); //a2
        assertEquals(10.0, testFixture.getContents(FQNAME + "Associate2").get(0).get("avg")); //a2
        assertEquals(30.0, testFixture.getContents(FQNAME + "Associate2").get(0).get("sum")); //a2
        assertEquals(3, testFixture.getContents(FQNAME + "Associate2").get(0).get("count")); //a2

        assertEquals(5.0, testFixture.getContents(FQNAME + "Associate2").get(1).get("min")); //b2
        assertEquals(15.0, testFixture.getContents(FQNAME + "Associate2").get(1).get("max")); //b2
        assertEquals(10.0, testFixture.getContents(FQNAME + "Associate2").get(1).get("avg")); //b2
        assertEquals(30.0, testFixture.getContents(FQNAME + "Associate2").get(1).get("sum")); //b2
        assertEquals(3, testFixture.getContents(FQNAME + "Associate2").get(1).get("count")); //b2

        assertEquals(5.0, testFixture.getContents(FQNAME + "Associate2").get(2).get("min")); //c2
        assertEquals(15.0, testFixture.getContents(FQNAME + "Associate2").get(2).get("max")); //c2
        assertEquals(10.0, testFixture.getContents(FQNAME + "Associate2").get(2).get("avg")); //c2
        assertEquals(30.0, testFixture.getContents(FQNAME + "Associate2").get(2).get("sum")); //c2
        assertEquals(3, testFixture.getContents(FQNAME + "Associate2").get(2).get("count")); //c2

    }

    @Test
    void testNavigatedAssociationWithAllNullToInfiniteCardinality(RdbmsDaoFixture testFixture, RdbmsDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();

        modelBuilder.addEntity("C")
                .withAttribute("Double", "cNumber");

        modelBuilder.addEntity("B")
                .withRelation("C", "toC", cardinality(0, -1));

        modelBuilder.addEntity("A")
                .withRelation("B", "toB", cardinality(0, -1))
                .withProperty("Double", "min", "self.toB.toC!min(m | m.cNumber)")
                .withProperty("Double", "max", "self.toB.toC!max(m | m.cNumber)")
                .withProperty("Double", "avg", "self.toB.toC!avg(a | a.cNumber)")
                .withProperty("Double", "sum", "self.toB.toC!sum(m | m.cNumber)")
                .withProperty("Integer", "count", "self.toB.toC!count(c)");

        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::A A = new demo::entities::A();\n" +

                        "var demo::entities::B B1 = new demo::entities::B();\n" +
                        "var demo::entities::B B2 = new demo::entities::B();\n" +

                        "var demo::entities::C C1 = new demo::entities::C(5);\n" +
                        "var demo::entities::C C2 = new demo::entities::C(10);\n" +
                        "var demo::entities::C C3 = new demo::entities::C(15);\n" +

                        "A.toB += B1;\n" +
                        "A.toB += B2;\n" +

                        "B1.toC += C1;\n" +
                        "B1.toC += C2;\n" +
                        "B1.toC += C3;\n" +

                        "B2.toC += C1;\n" +
                        "B2.toC += C2;\n" +
                        "B2.toC += C3;\n"
        );

        testFixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(testFixture.isInitialized(), "DAO initialized");

        run(testFixture);

        assertEquals(3, testFixture.getContents(FQNAME + "A").get(0).get("count"));
    }

    @Test
    public void testMultipleReferencedSumAndCount(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();

        final Model model = NamespaceBuilders.newModelBuilder().withName("M").build();

        final EntityType product = newEntityTypeBuilder()
                .withName("Product")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("price")
                        .withDataType(integerType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        product.setMapping(newMappingBuilder().withTarget(product).build());

        final EntityType orderItem = newEntityTypeBuilder()
                .withName("OrderItem")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("product")
                        .withLower(1).withUpper(1)
                        .withTarget(product)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        orderItem.setMapping(newMappingBuilder().withTarget(orderItem).build());

        final EntityType order = newEntityTypeBuilder()
                .withName("Order")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("items")
                        .withLower(0).withUpper(-1)
                        .withTarget(orderItem)
                        .withRelationKind(RelationKind.COMPOSITION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("totalNumberOfProducts")
                        .withDataType(integerType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.items!count()")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("numberOfProducts")
                        .withDataType(integerType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.items.product!count()")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("totalPrice")
                        .withDataType(integerType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.items!sum(i | i.product.price)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("sumOfUnitPrices")
                        .withDataType(integerType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.items.product!sum(p | p.price)")
                        .build())
                .build();
        order.setMapping(newMappingBuilder().withTarget(order).build());

        model.getElements().addAll(Arrays.asList(
                stringType, integerType, product, order, orderItem
        ));

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass productType = daoFixture.getAsmUtils().getClassByFQName("M._default_transferobjecttypes.Product").get();
        final EClass orderType = daoFixture.getAsmUtils().getClassByFQName("M._default_transferobjecttypes.Order").get();

        final Payload product1 = daoFixture.getDao().create(productType, Payload.map(
                "name", "Product1",
                "price", 22
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID product1Id = product1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Payload product2 = daoFixture.getDao().create(productType, Payload.map(
                "name", "Product2",
                "price", 35
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID product2Id = product2.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Payload product3 = daoFixture.getDao().create(productType, Payload.map(
                "name", "Product3",
                "price", 13
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID product3Id = product3.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        log.debug("Saved products: {}", Arrays.asList(product1Id, product2Id, product3Id));

        final Payload order1 = daoFixture.getDao().create(orderType, Payload.map(
                "items", Arrays.asList(
                        Payload.map("product", Payload.map(daoFixture.getIdProvider().getName(), product1Id)),
                        Payload.map("product", Payload.map(daoFixture.getIdProvider().getName(), product1Id)),
                        Payload.map("product", Payload.map(daoFixture.getIdProvider().getName(), product1Id)),
                        Payload.map("product", Payload.map(daoFixture.getIdProvider().getName(), product2Id))
                )
        ), null);

        log.debug("Saved order1: {}", order1);
        final int totalNumberOfProductsInOrder1 = order1.getAsCollectionPayload("items").size();
        final int numberOfProductsInOrder1 = order1.getAsCollectionPayload("items").stream()
                .map(i -> i.getAsPayload("product").getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()))
                .collect(Collectors.toSet())
                .size();
        final int totalPriceInOrder1 = order1.getAsCollectionPayload("items").stream()
                .mapToInt(i -> i.getAsPayload("product").getAs(Integer.class, "price"))
                .sum();
        final int sumOfUnitPricesInOrder1 = order1.getAsCollectionPayload("items").stream()
                .map(i -> i.getAsPayload("product"))
                .collect(Collectors.toMap(i -> i.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), i -> i.getAs(Integer.class, "price"), (x, y) -> x))
                .values().stream().mapToInt(Integer::valueOf).sum();

        assertThat(order1.getAs(Integer.class, "totalNumberOfProducts"), equalTo(totalNumberOfProductsInOrder1));
        assertThat(order1.getAs(Integer.class, "numberOfProducts"), equalTo(numberOfProductsInOrder1));
        assertThat(order1.getAs(Integer.class, "totalPrice"), equalTo(totalPriceInOrder1));
        assertThat(order1.getAs(Integer.class, "sumOfUnitPrices"), equalTo(sumOfUnitPricesInOrder1));
    }
}
