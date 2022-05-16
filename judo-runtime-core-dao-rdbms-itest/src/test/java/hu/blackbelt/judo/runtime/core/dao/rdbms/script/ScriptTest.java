package hu.blackbelt.judo.runtime.core.dao.rdbms.script;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.operation.OperationType;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.ScriptTestEntityBuilder;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;

import static hu.blackbelt.judo.meta.esm.operation.OperationType.INSTANCE;
import static hu.blackbelt.judo.meta.esm.operation.util.builder.OperationBuilders.newOperationBuilder;
import static hu.blackbelt.judo.meta.esm.operation.util.builder.OperationBuilders.newParameterBuilder;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.TRANSIENT;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.AGGREGATION;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newDataMemberBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newEntityTypeBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newMappingBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newOneWayRelationMemberBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newTransferObjectTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsMapWithSize.anEmptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class ScriptTest {

    private static final String DTO = "demo._default_transferobjecttypes.entities.";
    public static final String INPUT_THIS = "__this";
    public static final String OUTPUT = "output";

    public static Payload run(JudoRuntimeFixture fixture, String operationName, Payload exchange) {
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

    public static Payload run(JudoRuntimeFixture fixture) {
        return run(fixture, null, null);
    }

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        if (runtimeFixture.isInitialized()) {
            runtimeFixture.dropDatabase();
        }
    }

    @Test
    public void constructWithAttribute(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Cat").withAttribute("String", "categoryName");
        modelBuilder.addMappedTransferObject("CatInfo", "Cat").withAttribute("String", "categoryName");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::types::Integer i = 1\n" +
                        "var demo::services::CatInfo category = new demo::services::CatInfo(categoryName = 'category')\n" +
                        "var demo::types::String cat2Name = 'category2'\n" +
                        "var demo::services::CatInfo cat2 = new demo::services::CatInfo(categoryName = cat2Name)\n");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> cats = fixture.getContents(DTO + "Cat");
        assertThat(cats.size(), is(2));
        cats.sort(comparing(p -> p.getAs(String.class, "categoryName")));
        assertThat(cats.get(0).get("categoryName"), is("category"));
        assertThat(cats.get(1).get("categoryName"), is("category2"));
    }

    @Test
    public void constructNoParams(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Test");
        modelBuilder.addMappedTransferObject("TestInfo", "Test");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::services::TestInfo testInfo = new demo::services::TestInfo()");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        assertThat(fixture.getContents(DTO + "Test").size(), is(1));
    }

    @Test
    public void entityContainmentSingleCreate(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("ProductDetails")
                .withAttribute("String", "text");
        modelBuilder.addEntity("Product")
                .withContainment("ProductDetails", "details", cardinality(0, 1));
        modelBuilder.addMappedTransferObject("ProductDetailsInfo", "ProductDetails")
                .withAttribute("String", "text");
        modelBuilder.addMappedTransferObject("ProductInfo", "Product")
                .withContainment("ProductDetailsInfo", "details", cardinality(0, 1));
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::services::ProductInfo pi = new demo::services::ProductInfo(details = new demo::services::ProductDetailsInfo(text = 'textValue'))\n" +
                        "return pi")
                .withOutput("ProductInfo", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(fixture.getContents(DTO + "ProductDetails").size(), is(1));
        assertThat(fixture.getContents(DTO + "Product").size(), is(1));
        assertThat(output.get("__entityType"), is("demo.entities.Product"));
        assertThat(output.getAsPayload("details").get("__entityType"), is("demo.entities.ProductDetails"));
        assertThat(output.getAsPayload("details").get("text"), is("textValue"));

        Payload outputFromDb = fixture.getFromDb(output);
        assertThat(outputFromDb.getAsPayload("details").get("text"), is("textValue"));
    }

    @Test
    public void entityContainmentSingleSetUnset(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("ProductDetails")
                .withAttribute("String", "text");
        modelBuilder.addEntity("Product")
                .withContainment("ProductDetails", "details", cardinality(0, 1))
                .withAttribute("String", "name");
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Product product = new demo::entities::Product('product1')\n" +
                        "product.details = new demo::entities::ProductDetails('detailsText')\n" +
                        "var demo::entities::Product product2 = new demo::entities::Product('product2')\n" +
                        "product2.details = new demo::entities::ProductDetails('details2Text')\n" +
                        "unset product2.details");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents(DTO + "Product");
        contents.sort(comparing(p -> p.getAs(String.class, "name")));
        assertThat(contents.get(0).getAsPayload("details").get("text"), is("detailsText"));
        assertThat(contents.get(1).getAsPayload("details"), nullValue());
    }

    @Test
    public void entityContainmentChildSet(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("ProductDetails")
                .withAttribute("String", "text");
        modelBuilder.addEntity("Product")
                .withContainment("ProductDetails", "details", cardinality(0, 1))
                .withAttribute("String", "name");
        modelBuilder.addEntity("CustomProductDetails").withSuperType("ProductDetails").withAttribute("String", "customText");
        modelBuilder.addMappedTransferObject("CustomProductDetailsTo", "CustomProductDetails")
                .withAttribute("String", "text", "text")
                .withAttribute("String", "customText", "customText");
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Product product = new demo::entities::Product(name='product')\n" +
                        "new demo::services::CustomProductDetailsTo(text='productDetailsText', customText='customText')\n" +
                        "product.details = new demo::services::CustomProductDetailsTo(text='productDetailsText', customText='customText')");
        fixture.init(modelBuilder.build(), datasourceFixture);
        run(fixture);
        Payload productFromDb = fixture.getContents(DTO + "Product").get(0);
        assertThat(productFromDb.get("name"), is("product"));
        Payload productDetailsFromDb = fixture.getContents("demo.services.CustomProductDetailsTo").get(0);
        assertThat(productDetailsFromDb.get("customText"), is("customText"));
        assertThat(productDetailsFromDb.get("text"), is("productDetailsText"));
    }

    @Test
    public void entityReferenceSingleUnset(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Category").withAttribute("String", "categoryName");
        modelBuilder.addMappedTransferObject("CategoryInfo", "Category").withAttribute("String", "categoryName");
        modelBuilder.addEntity("Product")
                .withAttribute("String", "productName")
                .withRelation("Category", "category", cardinality(0, 1));
        modelBuilder.addMappedTransferObject("ProductInfo", "Product")
                .withAttribute("String", "productName")
                .withRelation("CategoryInfo", "category", cardinality(0, 1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::services::CategoryInfo category = new demo::services::CategoryInfo(categoryName = 'category1Name')\n" +
                        "var demo::services::CategoryInfo category2 = new demo::services::CategoryInfo(categoryName = 'category2Name')\n" +
                        "var demo::services::ProductInfo product1 = new demo::services::ProductInfo(productName = 'product1Name', category = category)\n" +
                        "var demo::services::ProductInfo product2 = new demo::services::ProductInfo(productName = 'product2Name', category = category)\n" +
                        "var demo::services::ProductInfo product3 = new demo::services::ProductInfo(productName = 'product3Name')\n" +
                        "product3.category = new demo::services::CategoryInfo(categoryName = 'category3Name')\n" +
                        "product2.category = category2\n" +
                        "unset product1.category\n" +
                        "unset product2.category");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents("demo.services.ProductInfo");
    }

    @Test
    public void entityContainmentMultiAdd(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("ProductDetails")
                .withAttribute("String", "text");
        modelBuilder.addEntity("Product")
                .withContainment("ProductDetails", "details", cardinality(0, -1));
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Product product = new demo::entities::Product()\n" +
                        "product.details += new demo::entities::ProductDetails('detailsText')");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents("demo._default_transferobjecttypes.entities.Product");
        assertThat(contents.get(0).getAsCollectionPayload("details").size(), is(1));
        assertThat(contents.get(0).getAsCollectionPayload("details").iterator().next().get("text"), is("detailsText"));
    }

    @Test
    public void entityContainmentMultiAddNewParam(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Course");
        modelBuilder.addEntity("Attendance")
                .withRelation("Course", "course", cardinality(1, 1));
        modelBuilder.addEntity("Student").withContainment("Attendance", "attendances", cardinality(0, -1));
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Course course = new demo::entities::Course()\n" +
                        "var demo::entities::Student student = new demo::entities::Student()\n" +
                        "student.attendances += new demo::entities::Attendance(course = course)");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
    }

    @Test
    public void setRelationsContainments(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("A")
                .withAttribute("String", "name");
        modelBuilder.addEntity("B")
                .withAggregation("A", "aAggregation", cardinality(0, -1))
                .withContainment("A", "aContainment", cardinality(0, -1))
                .withAggregation("A", "aAggregationSingle", cardinality(0, 1));
        modelBuilder.addUnmappedTransferObject("Result")
                .withContainment("A", "aResult", cardinality(0, -1))
                .withContainment("A", "aResult2", cardinality(0, -1));
        modelBuilder.addUnboundOperation("setRelations")
                .withOutput("Result", cardinality(1, 1))
                .withBody("" +
                        "var demo::entities::A a1 = new demo::entities::A(name = 'a1')\n" +
                        "var demo::entities::A a2 = new demo::entities::A()\n" +
                        "var demo::entities::B b1 = new demo::entities::B()\n" +
                        "b1.aAggregation = new demo::entities::A[] { a1 }\n" +
                        "b1.aAggregation = new demo::entities::A[] { a2, new demo::entities::A(name = 'a3'), new demo::entities::A(name = 'a3') }\n" +
                        "b1.aAggregationSingle = a1\n" +
                        "return new demo::services::Result(aResult = b1.aAggregation);");
        modelBuilder.addUnboundOperation("setContainments")
                .withOutput("Result", cardinality(1, 1))
                .withBody("" +
                        "var demo::entities::B b1 = new demo::entities::B()\n" +
                        "b1.aContainment = new demo::entities::A[] { new demo::entities::A(name = 'a1') }\n" +
                        "b1.aContainment = new demo::entities::A[] { new demo::entities::A(name = 'a2'), new demo::entities::A(name = 'a2') }\n" +
                        "var demo::entities::B b2 = new demo::entities::B()\n" +
                        "b2.aContainment += new demo::entities::A(name='a3')\n" +
                        "b2.aContainment += new demo::entities::A(name='a4')\n" +
                        "var demo::entities::A a34 = b2.aContainment!any()\n" +
                        "b2.aContainment = new demo::entities::A[] { new demo::entities::A(name = 'a5'), a34 }\n" +
                        "return new demo::services::Result(aResult = b1.aContainment, aResult2 = b2.aContainment);");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");

        Payload setRelationsResults = run(fixture, "setRelations", Payload.empty()).getAsPayload(OUTPUT);
        assertThat(setRelationsResults.getAsCollectionPayload("aResult").size(), is(3));
        assertThat(setRelationsResults.getAsCollectionPayload("aResult").stream().map(p -> p.getAs(String.class, "name")).filter(s -> "a1".equals(s)).count(), is(0L));
        assertThat(setRelationsResults.getAsCollectionPayload("aResult").stream().map(p -> p.getAs(String.class, "name")).filter(s -> "a3".equals(s)).count(), is(2L));

        Payload setContainmentsResult = run(fixture, "setContainments", Payload.empty()).getAsPayload(OUTPUT);
        assertThat(setContainmentsResult.getAsCollectionPayload("aResult").size(), is(2));
        assertThat(setContainmentsResult.getAsCollectionPayload("aResult").stream().map(p -> p.getAs(String.class, "name")).filter(s -> "a1".equals(s)).count(), is(0L));
        assertThat(setContainmentsResult.getAsCollectionPayload("aResult").stream().map(p -> p.getAs(String.class, "name")).filter(s -> "a2".equals(s)).count(), is(2L));
        assertThat(setContainmentsResult.getAsCollectionPayload("aResult2").size(), is(2));
    }

    @Test
    public void createUtoWithMtoParam(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity");

        modelBuilder.addEntity("Entity1")
                .withRelation("Entity", "e", cardinality(0, -1));
        modelBuilder.addMappedTransferObject("TransferEntity1", "Entity1")
                .withRelation("TransferEntity", "e", cardinality(0, -1));
        modelBuilder.addUnmappedTransferObject("TestUTO").withRelation("TransferEntity", "_test", cardinality(0, -1));
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::services::TransferEntity1 mto = new demo::services::TransferEntity1()\n" +
                        "mto.e += new demo::services::TransferEntity()\n" +
                        "mto.e += new demo::services::TransferEntity()\n" +
                        "var demo::services::TestUTO _e = new demo::services::TestUTO(_test = mto.e)");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
    }

    @Test
    public void testConstantDerived(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity")
                .withProperty("String", "derived", "'hello'");
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Entity e = new demo::entities::Entity()\n" +
                        "return e").withOutput("Entity", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("derived"), equalTo("hello"));
    }

    @Test
    public void testUndefinedEquals(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E").withAttribute("String", "e");
        modelBuilder.addEntity("Entity")
                .withAttribute("String", "text")
                .withAttribute("Boolean", "result")
                .withProperty("Boolean", "derived", "self.text == ''");
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Entity e = new demo::entities::Entity()\n" +
                        "e.result = e.text == '' // e.result should be undefined, as e.text is undefined\n" +
                        "return e").withOutput("Entity", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("result"), nullValue());
        assertThat(output.get("derived"), nullValue());
    }

    @Test
    public void entityDelete(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("ProductDetails").withAttribute("String", "text");
        modelBuilder.addMappedTransferObject("ProductDetailsInfo", "ProductDetails").withAttribute("String", "text");
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::services::ProductDetailsInfo obj1 = new demo::services::ProductDetailsInfo(text = 'obj1')\n" +
                        "var demo::services::ProductDetailsInfo obj2 = new demo::services::ProductDetailsInfo(text = 'obj2')\n" +
                        "delete obj1");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents("demo.services.ProductDetailsInfo");
        assertThat(contents.size(), is(1));
        assertThat(contents.get(0).get("text"), is("obj2"));
    }

    @Test
    public void entityDeleteViaReference(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity").withAttribute("String", "text");
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Entity e = new demo::entities::Entity('e text')\n" +
                        "var demo::entities::Entity e2 = e\n" +
                        "delete e\n" +
                        "return e2")
                .withOutput("Entity", cardinality(0, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture);
        assertThat(result.get(OUTPUT), is(Payload.empty()));
    }

    @Test
    public void entityCollectionDeleteFrom(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("ProductDetails").withAttribute("String", "text");
        modelBuilder.addUnboundOperation("deleteOne").withBody(
                "var demo::entities::ProductDetails obj1 = new demo::entities::ProductDetails('obj1')\n" +
                        "var demo::entities::ProductDetails obj2 = new demo::entities::ProductDetails('obj2')\n" +
                        "var demo::entities::ProductDetails[] objects\n" +
                        "objects += obj1\n" +
                        "objects += obj2\n" +
                        "delete obj1\n" +
                        "return objects")
                .withOutput("ProductDetails", cardinality(0, -1));
        modelBuilder.addUnboundOperation("getAll").withBody(
                "var demo::entities::ProductDetails[] allProducts = demo::entities::ProductDetails\n" +
                        "return allProducts")
                .withOutput("ProductDetails", cardinality(0, -1));
        modelBuilder.addUnboundOperation("deleteAll").withBody(
                "var demo::entities::ProductDetails[] allProducts = demo::services::UnboundServices.getAll()\n" +
                        "for (product in allProducts) delete product\n" +
                        "return allProducts")
                .withOutput("ProductDetails", cardinality(0, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture, "deleteOne", Payload.empty());
        assertThat(result.getAsCollectionPayload(OUTPUT).size(), is(1));
        assertThat(result.getAsCollectionPayload(OUTPUT).iterator().next().get("text"), is("obj2"));
        List<Payload> contents = fixture.getContents("demo._default_transferobjecttypes.entities.ProductDetails");
        assertThat(contents.size(), is(1));
        assertThat(contents.get(0).get("text"), is("obj2"));
        result = run(fixture, "getAll", Payload.empty());
        assertThat(result.getAsCollectionPayload(OUTPUT).size(), is(1));
        result = run(fixture, "deleteAll", Payload.empty());
        assertThat(result.getAsCollectionPayload(OUTPUT).size(), is(0));
        contents = fixture.getContents("demo._default_transferobjecttypes.entities.ProductDetails");
        assertThat(contents.size(), is(0));
    }

    @Test
    public void entityUnboundAttribute(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("ProductDetails").withAttribute("String", "text").withAttribute("String", "text2");
        modelBuilder.addMappedTransferObject("ProductDetailsInfo", "ProductDetails")
                .withAttribute("String", "b")
                .withAttribute("String", "c")
                .withAttribute("String", "text");
        modelBuilder.addMappedTransferObject("ProductDetailsInfo2", "ProductDetails")
                .withAttribute("String", "d")
                .withAttribute("String", "text")
                .withAttribute("String", "text2");
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::services::ProductDetailsInfo e = new demo::services::ProductDetailsInfo(text = 'q', b = 'eb')\n\n" +
                        "e.text = 'text value'\n" +
                        "e.c = 'ec'\n" +
                        "var demo::services::ProductDetailsInfo2 e2 = demo::services::ProductDetailsInfo2!sort()!head()\n" +
                        "e2.text2 = 'e2 text2'\n" +
                        "e2.text = e.text\n" +
                        "return e")
                .withOutput("ProductDetailsInfo", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("c"), is("ec"));
        assertThat(output.get("b"), is("eb"));
        assertThat(output.get("text2"), nullValue());
        assertThat(output.get("text"), is("text value"));
        assertThat(output.get("__entityType"), is("demo.entities.ProductDetails"));
        Payload entity = fixture.getContents("demo._default_transferobjecttypes.entities.ProductDetails").get(0);
        assertThat(entity.get("text"), is("text value"));
        assertThat(entity.get("text2"), is("e2 text2"));
    }

    @Test
    public void entityUnboundRelation(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Related");
        modelBuilder.addEntity("Entity");
        modelBuilder
                .addMappedTransferObject("Mto", "Entity")
                .withRelation("Related", "related", cardinality(0, 1))
                .withRelation("Related", "relateds", cardinality(0, -1))
        ;
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::services::Mto mto = new demo::services::Mto()\n" +
                        "var demo::entities::Related r = new demo::entities::Related()\n" +
                        "mto.related = new demo::entities::Related()\n" +
                        "mto.related = mto.related\n" +
                        "mto.related = r\n" +
                        "mto.relateds += mto.related\n" +
                        "mto.relateds += new demo::entities::Related()\n" +
                        "mto.relateds += r\n" +
                        "mto.relateds += new demo::entities::Related[] { r, mto.related, new demo::entities::Related(), new demo::entities::Related() }\n" +
                        "return mto")
                .withOutput("Mto", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAsPayload("related").get("__entityType"), is("demo.entities.Related"));
        assertThat(output.getAsCollectionPayload("relateds"), hasSize(4));
        assertThat(output.getAsCollectionPayload("relateds").stream().filter(p -> p.containsKey("__entityType")).count(), is(4L));
        Payload entity = fixture.getContents("demo._default_transferobjecttypes.entities.Entity").get(0);
    }

    @Test
    public void entityCollectionDeleteMember(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity").withAttribute("String", "text");
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Entity[] es\n" +
                        "var demo::entities::Entity e1 = new demo::entities::Entity('e1')\n" +
                        "es += e1\n" +
                        "es += new demo::entities::Entity('e2')\n" +
                        "delete e1\n" +
                        "return es")
                .withOutput("Entity", cardinality(1, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Collection<Payload> result = run(fixture).getAsCollectionPayload(OUTPUT);
        assertThat(result.size(), is(1));
        assertThat(result.iterator().next().get("text"), is("e2"));
    }

    @Test
    public void entityDeleteViaUnmappedTransferObject(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity").withAttribute("String", "text");
        modelBuilder.addUnmappedTransferObject("TO").withRelation("Entity", "e", cardinality(0, 1));
        modelBuilder.addUnmappedTransferObject("TO2").withRelation("TO", "to", cardinality(0, 1));
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Entity e = new demo::entities::Entity('e text')\n" +
                        "var demo::services::TO to = new demo::services::TO()\n" +
                        "var demo::services::TO2 to2 = new demo::services::TO2()\n" +
                        "to2.to = to\n" +
                        "to.e = e\n" +
                        "delete e\n" +
                        "return to2")
                .withOutput("TO2", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat("deleting the entity should remove it from the unmapped transfer objects", output.getAsPayload("to").get("e"), nullValue());
    }

    @Test
    public void entityDeleteViaMappedTransferObjectViaOperationCall(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity").withAttribute("String", "text");
        modelBuilder.addEntity("TO")
                .withAggregation("Entity", "e", cardinality(0, 1))
                .withAggregation("Entity", "f", cardinality(0, 1));
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Entity e = new demo::entities::Entity('e text')\n" +
                        "var demo::entities::TO to = new demo::entities::TO()\n" +
                        "to.e = e\n" +
                        "to.f = new demo::entities::Entity('f text')\n" +
                        "demo::services::UnboundServices.deleteE(to)\n" +
                        "return to")
                .withOutput("TO", cardinality(1, 1));
        modelBuilder.addUnboundOperation("deleteE").withBody(
                "delete (mutable input.e)")
                .withInput("TO", "input", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture, "init", Payload.empty());
        assertThat(((Payload) result.get(OUTPUT)).get("e"), nullValue());
        assertThat(((Payload) ((Payload) result.get(OUTPUT)).get("f")).get("text"), is("f text"));
    }

    @Test
    public void entityDeleteViaUnmappedTransferObjectViaOperationCall(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity").withAttribute("String", "text");
        modelBuilder.addUnmappedTransferObject("TO")
                .withRelation("Entity", "e", cardinality(0, 1))
                .withRelation("Entity", "f", cardinality(0, 1));
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Entity e = new demo::entities::Entity('e text')\n" +
                        "var demo::services::TO to = new demo::services::TO()\n" +
                        "to.e = e\n" +
                        "to.f = new demo::entities::Entity('f text')\n" +
                        "demo::services::UnboundServices.deleteE(to)\n" +
                        "return to")
                .withOutput("TO", cardinality(1, 1));
        modelBuilder.addUnboundOperation("deleteE").withBody(
                "delete input.e")
                .withInput("TO", "input", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture, "init", Payload.empty());
        assertThat(((Payload) result.get(OUTPUT)).get("e"), nullValue());
        assertThat(((Payload) ((Payload) result.get(OUTPUT)).get("f")).get("text"), is("f text"));
    }

    @Test
    public void entityDeleteFromUnmappedTransferObjectCollection(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity").withAttribute("String", "text");
        modelBuilder.addUnmappedTransferObject("TO")
                .withRelation("Entity", "es", cardinality(0, -1))
                .withRelation("Entity", "fs", cardinality(0, -1));
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Entity e1 = new demo::entities::Entity('e1')\n" +
                        "var demo::entities::Entity e2 = new demo::entities::Entity('e2')\n" +
                        "var demo::services::TO to = new demo::services::TO()\n" +
                        "to.es += e1\n" +
                        "to.es += e2\n" +
                        "to.fs += new demo::entities::Entity('f text')\n" +
                        "demo::services::UnboundServices.deleteEs(to)\n" +
                        "return to")
                .withOutput("TO", cardinality(1, 1));
        modelBuilder.addUnboundOperation("deleteEs").withBody(
                "for (e in input.es) { delete e }")
                .withInput("TO", "input", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture, "init", Payload.empty());
        assertThat(result.getAs(Payload.class, OUTPUT).getAsCollectionPayload("es"), empty());
        assertThat(result.getAs(Payload.class, OUTPUT).getAsCollectionPayload("fs").iterator().next().get("text"), is("f text"));
    }

    @Test
    public void entityDeleteViaNavigation(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity").withAttribute("String", "text");
        modelBuilder.addEntity("TO")
                .withAggregation("Entity", "e", cardinality(0, 1));
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Entity e = new demo::entities::Entity('e text')\n" +
                        "var demo::entities::TO to = new demo::entities::TO()\n" +
                        "to.e = e\n" +
                        "delete to.e\n" +
                        "return to").withOutput("TO", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture, "init", Payload.empty());
        List<Payload> contents = fixture.getContents("demo._default_transferobjecttypes.entities.Entity");
        assertThat(contents.size(), is(0));
    }

    @Test
    public void selectAll(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("ProductDetails").withAttribute("String", "text");
        modelBuilder.addMappedTransferObject("ProductDetailsInfo", "ProductDetails").withAttribute("String", "text");
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::services::ProductDetailsInfo obj1 = new demo::services::ProductDetailsInfo(text = 'obj1')\n" +
                        "var demo::services::ProductDetailsInfo obj2 = new demo::services::ProductDetailsInfo(text = 'obj2')\n" +
                        "var demo::services::ProductDetailsInfo[] products = demo::services::ProductDetailsInfo\n" +
                        "for (product in products) product.text = 'a'\n" +
                        "for (product in demo::services::ProductDetailsInfo) { product.text = 'b' }");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents("demo.services.ProductDetailsInfo");
        assertThat(contents.size(), is(2));
        assertThat(contents.stream().filter(p -> "b".equals(p.get("text"))).count(), is(2L));
    }

    @Test
    public void returnNew(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Category").withAttribute("String", "categoryName");
        modelBuilder.addUnboundOperation("initializer", "return new demo::entities::Category('categoryName')", "Category", cardinality(0, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        assertThat(run(fixture).getAsPayload(OUTPUT).get("categoryName"), is("categoryName"));
    }

    @Test
    public void returnNewCollectionFromIf(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Category").withAttribute("String", "categoryName");
        modelBuilder.addUnboundOperation("initializer", "" +
                "if (true) { return new demo::entities::Category[] { new demo::entities::Category('categoryName') } }\n" +
                "return new demo::entities::Category[] { new demo::entities::Category('cat2') }", "Category", cardinality(0, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        assertThat(run(fixture).getAsCollectionPayload(OUTPUT).size(), is(1));

    }

    @Test
    public void returnObjectNavigation(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Category").withAttribute("String", "categoryName");
        modelBuilder.addEntity("Product")
                .withRelation("Category", "category", cardinality(0, 1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::Category c = new demo::entities::Category('categoryName')\n" +
                        "var demo::entities::Product p = new demo::entities::Product(category = c)\n" +
                        "return p.category").withOutput(
                "Category", cardinality(0, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        assertThat(run(fixture).getAsPayload(OUTPUT).get("categoryName"), is("categoryName"));
    }

    @Test
    public void returnCollection(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Product").withAttribute("String", "productName");
        modelBuilder.addEntity("Category")
                .withAttribute("String", "categoryName")
                .withRelation("Product", "products", cardinality(0, -1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::Category c = new demo::entities::Category('categoryName')\n" +
                        "c.products += new demo::entities::Product('p1')\n" +
                        "c.products += new demo::entities::Product('p2')\n" +
                        "var demo::entities::Product[] products = c.products\n" +
                        "return products").withOutput(
                "Product", cardinality(0, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        List<Payload> output = run(fixture).getAsCollectionPayload(OUTPUT).stream().sorted(comparing(p -> p.getAs(String.class, "productName"))).collect(toList());
        assertThat(output.get(0).get("productName"), is("p1"));
        assertThat(output.get(1).get("productName"), is("p2"));
    }

    @Test
    public void returnCollectionNavigation(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Product").withAttribute("String", "productName");
        modelBuilder.addEntity("Category")
                .withAttribute("String", "categoryName")
                .withRelation("Product", "products", cardinality(0, -1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::Category c = new demo::entities::Category('categoryName')\n" +
                        "c.products += new demo::entities::Product('p1')\n" +
                        "c.products += new demo::entities::Product('p2')\n" +
                        "return c.products").withOutput(
                "Product", cardinality(0, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        List<Payload> output = run(fixture).getAsCollectionPayload(OUTPUT).stream().sorted(comparing(p -> p.getAs(String.class, "productName"))).collect(toList());
        assertThat(output.get(0).get("productName"), is("p1"));
        assertThat(output.get(1).get("productName"), is("p2"));
    }

    @Test
    public void returnCollectionFromCollectionNavigation(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Product").withAttribute("String", "productName");
        modelBuilder.addEntity("Category")
                .withAttribute("String", "categoryName")
                .withRelation("Product", "products", cardinality(0, -1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::Category c = new demo::entities::Category('categoryName')\n" +
                        "c.products += new demo::entities::Product('p1')\n" +
                        "c.products += new demo::entities::Product('p2')\n" +
                        "var demo::entities::Category[] cs = new demo::entities::Category[] { c }\n" +
                        "return cs.products").withOutput(
                "Product", cardinality(0, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        List<Payload> output = run(fixture).getAsCollectionPayload(OUTPUT).stream().sorted(comparing(p -> p.getAs(String.class, "productName"))).collect(toList());
        assertThat(output.get(0).get("productName"), is("p1"));
        assertThat(output.get(1).get("productName"), is("p2"));
    }

    @Test
    public void multipleStepNavigationRemove(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Address");
        modelBuilder.addEntity("Person").withAggregation("Address", "addresses", cardinality(0, -1));
        modelBuilder.addEntity("Stage").withRelation("Person", "responsible", cardinality(0, 1));
        modelBuilder.addEntity("Contract").withContainment("Stage", "openStage", cardinality(0, 1));
        modelBuilder.addBoundOperation("Contract", "getAddresses").withBody(
                "var demo::entities::Address a = new demo::entities::Address()\n" +
                "this->openStage->responsible=>addresses -= a\n" +
                "return this"
        ).withOutput("Contract", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
    }

    @Test
    public void numericFunction(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity")
                .withAttribute("Double", "value")
                .withAttribute("Double", "value2")
                .withProperty("Double", "derived", "self.value * 2");
        modelBuilder.addUnboundOperation("initializer", "" +
                "var demo::entities::Entity e = new demo::entities::Entity(value = 3.14, value2 = 0)\n" +
                "e.value2 = e.derived!round() + 1\n" +
                "e.value = e.value!round()\n" +
                "return e", "Entity");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("value"), is(3.0));
        assertThat(output.get("value2"), is(7.0));
        assertThat(output.get("derived"), is(6.0));
    }

    @Test
    public void constructWithRelation(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Category").withAttribute("String", "categoryName");
        modelBuilder.addMappedTransferObject("CategoryInfo", "Category").withAttribute("String", "categoryName");
        modelBuilder.addEntity("Product")
                .withAttribute("String", "productName")
                .withRelation("Category", "category", cardinality(0, 1));
        modelBuilder.addMappedTransferObject("ProductInfo", "Product")
                .withAttribute("String", "productName")
                .withAggregation("CategoryInfo", "category", cardinality(0, 1));
        modelBuilder.addUnboundOperation("initializer",
                "var demo::types::String categoryName = 'C'" +
                        "var demo::services::CategoryInfo category = new demo::services::CategoryInfo(categoryName = categoryName + categoryName)\n" +
                        "var demo::services::CategoryInfo category2 = new demo::services::CategoryInfo(categoryName = 'C2')\n" +
                        "var demo::services::ProductInfo product = new demo::services::ProductInfo(productName = 'p1', category = category)\n" +
                        "var demo::services::ProductInfo product2 = new demo::services::ProductInfo(productName = 'p2', category = category)\n" +
                        "product2.category = category2\n" +
                        "return category", "CategoryInfo");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("categoryName"), is("CC"));
        List<Payload> contents = fixture.getContents("demo.services.ProductInfo").stream().sorted(comparing(p -> p.getAs(String.class, "productName"))).collect(toList());
        assertThat(contents.get(0).getAsPayload("category").get("categoryName"), is("CC"));
        assertThat(contents.get(1).getAsPayload("category").get("categoryName"), is("C2"));
    }

    @Test
    public void updateAttribute(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Address").withAttribute("String", "address");
        modelBuilder.addMappedTransferObject("AddressInfo", "Address").withAttribute("String", "address");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::services::AddressInfo address = new demo::services::AddressInfo(address = 'address_original')\n" +
                        "address.address = 'address_modified1'");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents("demo.services.AddressInfo");
        assertThat(contents.iterator().next().get("address"), is("address_modified1"));
    }

    @Test
    public void updateDoubleAttribute(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("DoubleWrapper").withAttribute("Double", "value");
        modelBuilder.addMappedTransferObject("DoubleWrapperInfo", "DoubleWrapper").withAttribute("Double", "value");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::services::DoubleWrapperInfo wrapper = new demo::services::DoubleWrapperInfo(value = 3.14)\n" +
                        "if (wrapper.value < 4) { wrapper.value = wrapper.value + 5 }\n" +
                        "if (wrapper.value > 8) wrapper.value = 0\n" +
                        "while (wrapper.value < 2 and wrapper.value > -1.5) wrapper.value = wrapper.value + 1");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents("demo.services.DoubleWrapperInfo");
        assertThat(contents.get(0).get("value"), is(2.0));
    }

    @Test
    public void updateVariable(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("DoubleWrapper").withAttribute("Double", "value");
        modelBuilder.addMappedTransferObject("DoubleWrapperInfo", "DoubleWrapper").withAttribute("Double", "value");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::types::Double a = 3.14\n" +
                        "if (a < 4) { a = 2*a }\n" +
                        "new demo::services::DoubleWrapperInfo(value = a)");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents("demo.services.DoubleWrapperInfo");
        assertThat(contents.get(0).get("value"), is(6.28));
    }

    @Test
    public void unmappedTransferObject(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity").withAttribute("String", "name");
        modelBuilder.addMappedTransferObject("EntityInfo", "Entity").withAttribute("String", "name");
        modelBuilder.addUnmappedTransferObject("Text").withAttribute("String", "text");
        modelBuilder.addUnmappedTransferObject("Data")
                .withAttribute("String", "name")
                .withRelation("EntityInfo", "entity", cardinality(0, 1))
                .withRelation("EntityInfo", "entities", cardinality(0, -1))
                .withRelation("Text", "text", cardinality(0, 1))
                .withRelation("Text", "texts", cardinality(0, -1))
                .withRelation("Text", "texts2", cardinality(0, -1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::services::Data data = new demo::services::Data(name = 'data1')\n" +
                        "data.name = 'data1 updated'\n" +
                        "data.entity = new demo::services::EntityInfo('entity1')\n" +
                        "data.entities += new demo::services::EntityInfo('entity3')\n" +
                        "data.entities = new demo::services::EntityInfo[] { new demo::services::EntityInfo('entity2') }\n" +
                        "var demo::services::EntityInfo entity4 = new demo::services::EntityInfo('entity4')\n" +
                        "data.entities += entity4\n" +
                        "data.text = new demo::services::Text('text1')\n" +
                        "data.texts += data.text\n" +
                        "data.texts -= data.text\n" +
                        "data.texts2 += new demo::services::Text[] { data.text }\n" +
                        "for (t in data.texts2) { data.texts2 -= t }\n" +
                        "unset data.text\n" +
                        "return data").withOutput("Data", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        Collection<Payload> entities = output.getAsCollectionPayload("entities");
        assertThat(entities.size(), is(2));
        assertThat(entities.stream().filter(p -> "demo.entities.Entity".equals(p.getAs(String.class, "__entityType"))).count(), is(2L));
        assertThat(output.getAsPayload("entity").get("__entityType"), is("demo.entities.Entity"));
        assertThat(output.getAsCollectionPayload("texts").size(), is(0));
        assertThat(output.getAsCollectionPayload("texts2").size(), is(0));
        assertTrue(Objects.isNull(output.get("text")));
    }

    @Test
    public void boundOperation(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TextWrapper").withAttribute("String", "text");
        modelBuilder.addEntity("Template").withAttribute("String", "text");
        modelBuilder.addEntity("Author").withAttribute("String", "name");
        modelBuilder.addEntity("Comment")
                .withAttribute("String", "text")
                .withProperty("String", "fancyText", "self.text + ' derived'")
                .withRelation("Author", "authors", cardinality(0, -1));
        modelBuilder.addBoundOperation("Comment", "init")
                .withOutput("Comment", cardinality(1, 1))
                .withBody("" +
                        "this.text = 'commentTextValue'\n" +
                        "var demo::types::String s = this.fancyText\n" +
                        "this.authors += new demo::entities::Author('author1Name')\n" +
                        "this.authors += new demo::entities::Author('author2Name')\n" +
                        "for (author in this.authors) { author.name = author.name + ' update ' + this.fancyText }\n" +
                        "return this");
        modelBuilder.addBoundOperation("Comment", "initWithParam")
                .withOutput("Comment", cardinality(1, 1))
                .withInput("TextWrapper", "template", cardinality(1, 1))
                .withBody("var demo::entities::Comment result = new demo::entities::Comment(text = template.text)\n" +
                        "result.text = template.text + ' done'\n" +
                        "return result");
        modelBuilder.addBoundOperation("Comment", "callInit")
                .withOutput("Comment", cardinality(1, 1))
                .withBody("this.init()\n" +
                        "var demo::entities::Comment result = this.init()\n" +
                        "result = result.init()\n" +
                        "result = result.initWithParam(new demo::services::TextWrapper('initWithParam text'))\n" +
                        "return result");
        Model model = modelBuilder.build();

        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture, "initWithParam", Payload.map("template", Payload.map("text", "templateTextValue")));
        List<Payload> contents = fixture.getContents("demo._default_transferobjecttypes.entities.Comment");
        assertThat(contents.get(0).get("fancyText"), is("templateTextValue done derived"));
        run(fixture, "init", Payload.map(INPUT_THIS, result.get(OUTPUT)));
        Payload callInitResult = run(fixture, "callInit", Payload.map(INPUT_THIS, result.get(OUTPUT))).getAsPayload(OUTPUT);
        assertThat(callInitResult.get("text"), is("initWithParam text done"));
        assertThat(callInitResult.get("fancyText"), is("initWithParam text done derived"));
    }

    @Test
    public void callReturnsMulti(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addEntity("Entity").withAttribute("String", "text");
        builder.addUnboundOperation("init")
                .withBody(
                        "var demo::entities::Entity e1 = new demo::entities::Entity('e1 text')\n" +
                                "return new demo::entities::Entity[] { new demo::entities::Entity('e2 text'), e1 }\n")
                .withOutput("Entity", cardinality(1, -1));
        builder.addUnboundOperation("init2")
                .withBody(
                        "var demo::entities::Entity[] es = demo::services::UnboundServices.init()\n" +
                                "return es\n")
                .withOutput("Entity", cardinality(1, -1));
        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        List<Payload> output = run(fixture, "init2", Payload.empty()).getAsCollectionPayload(OUTPUT).stream().sorted(comparing(p -> p.getAs(String.class, "text"))).collect(toList());
        assertThat(output.get(0).get("text"), is("e1 text"));
        assertThat(output.get(1).get("text"), is("e2 text"));
    }

    @Test
    public void callParamMulti(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addEntity("Entity").withAttribute("String", "text");
        builder.addUnboundOperation("init")
                .withBody(
                        "var immutable demo::entities::Entity[] es = input\n" +
                                "return mutable es\n")
                .withInput("Entity", "input", cardinality(1, -1))
                .withOutput("Entity", cardinality(1, -1));
        builder.addUnboundOperation("init2")
                .withBody(
                        "var demo::entities::Entity[] es\n" +
                                "es += new demo::entities::Entity('e1')\n" +
                                "es += new demo::entities::Entity('e2')\n" +
                                "return demo::services::UnboundServices.init(immutable es)\n")
                .withOutput("Entity", cardinality(1, -1));
        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        List<Payload> output = run(fixture, "init2", Payload.empty()).getAsCollectionPayload(OUTPUT).stream().sorted(comparing(p -> p.getAs(String.class, "text"))).collect(toList());
        assertThat(output.get(0).get("text"), is("e1"));
        assertThat(output.get(1).get("text"), is("e2"));
    }

    @Test
    public void inputAny(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addEntity("Entity").withAttribute("String", "text");
        builder.addUnboundOperation("init")
                .withBody(
                        "if (input!isDefined()) { }\n")
                .withInput("Entity", "input", cardinality(0, 1));
        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture, "init", Payload.empty());
    }

    @Test
    public void collectionCount(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addEntity("Entity").withAttribute("String", "text");
        builder.addUnboundOperation("init")
                .withBody(
                        "var demo::types::Integer i = input!count()")
                .withInput("Entity", "input", cardinality(0, -1));
        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture, "init", Payload.map("input", Collections.EMPTY_SET));
    }

    @Test
    public void collectionEmpty(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addEntity("Entity").withAttribute("String", "text");
        builder.addUnboundOperation("init")
                .withBody(
                        "var demo::types::Boolean b = input!empty()")
                .withInput("Entity", "input", cardinality(0, -1));
        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture, "init", Payload.map("input", Collections.EMPTY_SET));
    }

    @Test
    public void stringSubstring(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addEntity("Entity")
                .withAttribute("String", "text")
                .withAttribute("String", "textFirst")
                .withAttribute("String", "textLast")
                .withAttribute("String", "textSubstring");
        builder.addUnboundOperation("init")
                .withBody(
                        "var demo::entities::Entity e = new demo::entities::Entity(text = 'apple')\n" +
                                "var demo::types::Integer i = 1\n" +
                                "var demo::types::String s = e.text!first(2)\n" +
                                "e.textFirst = s\n" +
                                "e.textLast = e.text!last(2)\n" +
                                "e.textSubstring = e.text!substring(2,3)\n" +
                                "return e")
                .withOutput("Entity", cardinality(1, 1));
        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture, "init", Payload.empty()).getAsPayload("output");
        assertThat(output.getAs(String.class, "textFirst"), is("ap"));
        assertThat(output.getAs(String.class, "textLast"), is("le"));
        assertThat(output.getAs(String.class, "textSubstring"), is("ppl"));

    }

    @Test
    public void stringReplace(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addEntity("Entity")
                .withAttribute("String", "text")
                .withAttribute("String", "textReplaced");
        builder.addUnboundOperation("init")
                .withBody(
                        "var demo::entities::Entity e = new demo::entities::Entity(text = 'apple')\n" +
                                "e.textReplaced = e.text!replace('le', 'endix')\n" +
                                "return e")
                .withOutput("Entity", cardinality(1, 1));
        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture, "init", Payload.empty()).getAsPayload("output");
        assertThat(output.getAs(String.class, "textReplaced"), is("appendix"));
    }

    @Test
    public void setEmptyCollectionAsReference(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E").withAttribute("String", "e");
        modelBuilder.addEntity("Entity")
                .withProperty("Integer", "derived", "self.entities!count()")
                .withRelation("E", "entities", cardinality(0, -1));
        modelBuilder.addUnboundOperation("init", "" +
                "var demo::entities::Entity e = new demo::entities::Entity()\n" +
                "e.entities = new demo::entities::E[] { }\n" +
                "return e.entities", "E", cardinality(1, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        assertThat(run(fixture, "init", Payload.empty()).getAsCollectionPayload(OUTPUT), empty());
    }

    @Test
    public void returnUnmappedToFromCall(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addEntity("Entity");
        builder.addUnmappedTransferObject("Unmapped").withAttribute("String", "text");
        builder.addUnboundOperation("init").withBody("var demo::entities::Entity e = new demo::entities::Entity(); return e").withOutput("Entity", cardinality(1, 1));
        builder.addBoundOperation("Entity", "createUnmapped")
                .withOutput("Unmapped", cardinality(1, 1))
                .withBody("return new demo::services::Unmapped('created unmapped')");
        builder.addBoundOperation("Entity", "getUnmapped")
                .withOutput("Unmapped", cardinality(1, 1))
                .withBody(
                        "var demo::services::Unmapped result = this.createUnmapped()\n" +
                                "return this.createUnmapped()");
        builder.addBoundOperation("Entity", "getUnmapped2")
                .withOutput("Unmapped", cardinality(1, 1))
                .withInput("Unmapped", "input", cardinality(1, 1))
                .withBody(
                        "var demo::services::Unmapped result = mutable input\n" +
                                "return result");
        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload initResult = run(fixture, "init", Payload.empty()).getAsPayload(OUTPUT);
        assertThat(initResult.get("__entityType"), is("demo.entities.Entity"));
        Payload unmappedResult = run(fixture, "getUnmapped", Payload.map(INPUT_THIS, initResult)).getAsPayload(OUTPUT);
        assertThat(unmappedResult.get("text"), is("created unmapped"));
        Payload result = run(fixture, "getUnmapped2", Payload.map(INPUT_THIS, initResult, "input", unmappedResult)).getAsPayload(OUTPUT);
        assertThat(result.get("text"), is("created unmapped"));
    }

    @Test
    public void updateRelationAttribute(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Address").withAttribute("String", "address");
        modelBuilder.addMappedTransferObject("AddressInfo", "Address").withAttribute("String", "address");
        modelBuilder.addEntity("Customer")
                .withRelation("Address", "address", cardinality(0, 1));
        modelBuilder.addMappedTransferObject("CustomerInfo", "Customer")
                .withRelation("AddressInfo", "address", cardinality(0, 1));
        modelBuilder.addEntity("Directory")
                .withRelation("Customer", "customer", cardinality(0, 1));
        modelBuilder.addMappedTransferObject("DirectoryInfo", "Directory")
                .withRelation("CustomerInfo", "customer", cardinality(0, 1));

        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::services::AddressInfo address = new demo::services::AddressInfo(address = 'address_original')\n" +
                        "var demo::services::CustomerInfo customer = new demo::services::CustomerInfo(address = address)\n" +
                        "address.address = 'address_modified1'\n" +
                        "customer.address.address = 'address_modified2'\n" +
                        "var demo::services::DirectoryInfo directory = new demo::services::DirectoryInfo(customer = customer)\n" +
                        "directory.customer.address.address = 'address_modified3'");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents("demo.services.CustomerInfo", "address");
        assertThat(contents.get(1).get("address"), is("address_modified3"));
    }

    @Test
    public void updateMulti(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Author").withAttribute("String", "name");
        modelBuilder.addMappedTransferObject("AuthorInfo", "Author").withAttribute("String", "name");
        modelBuilder.addEntity("Book")
                .withAttribute("String", "title")
                .withRelation("Author", "authors", cardinality(0, -1));
        modelBuilder.addMappedTransferObject("BookInfo", "Book")
                .withAttribute("String", "title")
                .withAggregation("AuthorInfo", "authors", cardinality(0, -1));
        modelBuilder.addEntity("BookItem")
                .withRelation("Book", "book", cardinality(0, 1));
        modelBuilder.addMappedTransferObject("BookItemInfo", "BookItem")
                .withAggregation("BookInfo", "book", cardinality(0, 1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::services::AuthorInfo author1 = new demo::services::AuthorInfo(name = 'author1Name')\n" +
                        "var demo::services::BookInfo book = new demo::services::BookInfo(title = 'bookTitle1')\n" +
                        "book.authors += author1\n" +
                        "book.authors += new demo::services::AuthorInfo(name = 'author2Name')\n" +
                        "var demo::services::BookInfo book2 = new demo::services::BookInfo(title = 'bookTitle2')\n" +
                        "book2.authors += author1\n" +
                        "book2.authors += new demo::services::AuthorInfo(name = 'author2Name2')\n" +
                        "book2.authors -= author1\n" +
                        "var demo::services::BookItemInfo bookItem = new demo::services::BookItemInfo(book = book)\n" +
                        "bookItem.book = book2\n" +
                        "bookItem.book.authors += new demo::services::AuthorInfo(name = 'author3Name')\n" +
                        "return bookItem").withOutput("BookItemInfo", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAsPayload("book").getAsCollectionPayload("authors").size(), is(2));
        assertThat(output.getAsPayload("book").getAsCollectionPayload("authors").stream().sorted(comparing(p -> p.getAs(String.class, "name"))).collect(toList()).get(1).get("name"), is("author3Name"));
        List<Payload> contents = fixture.getContents("demo.services.BookInfo");
        assertThat(contents.size(), is(2));
        contents.sort(comparing(p -> p.getAs(String.class, "title")));
        assertThat(contents.get(0).getAsCollectionPayload("authors").size(), is(2));
        assertThat(contents.get(0).getAsCollectionPayload("authors").stream().sorted(comparing(p -> p.getAs(String.class, "name"))).collect(toList()).get(0).get("name"), is("author1Name"));
        assertThat(contents.get(1).getAsCollectionPayload("authors").stream().sorted(comparing(p -> p.getAs(String.class, "name"))).collect(toList()).get(0).get("name"), is("author2Name2"));
    }

    @Test
    public void testUndefined(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addEntity("Entity").withAttribute("String", "text");
        builder.addEntity("Entity2").withRelation("Entity", "entity", cardinality(0, 1));
        builder.addUnboundOperation("initializer").withBody(
                "var demo::entities::Entity e = new demo::entities::Entity()\n" +
                        "var demo::entities::Entity2 e2 = new demo::entities::Entity2()\n" +
                        "var demo::types::Boolean b1 = e.text!isDefined()\n" +
                        "b1 = e2.entity!isDefined()");
        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
    }

    @Test
    public void kleene(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addUnmappedTransferObject("Input")
            .withAttribute("Boolean", "t")
            .withAttribute("Boolean", "f")
            .withAttribute("Boolean", "u");
        builder.addUnmappedTransferObject("Output")
                // true vs true
                .withAttribute("Boolean", "t_or_t")
                .withAttribute("Boolean", "t_and_t")
                .withAttribute("Boolean", "t_xor_t")
                .withAttribute("Boolean", "t_implies_t")
                // true vs false
                .withAttribute("Boolean", "t_or_f")
                .withAttribute("Boolean", "t_and_f")
                .withAttribute("Boolean", "t_xor_f")
                .withAttribute("Boolean", "t_implies_f")
                // true vs undefined
                .withAttribute("Boolean", "t_or_u")
                .withAttribute("Boolean", "t_and_u")
                .withAttribute("Boolean", "t_xor_u")
                .withAttribute("Boolean", "t_implies_u")
                // false vs true
                .withAttribute("Boolean", "f_or_t")
                .withAttribute("Boolean", "f_and_t")
                .withAttribute("Boolean", "f_xor_t")
                .withAttribute("Boolean", "f_implies_t")
                // false vs false
                .withAttribute("Boolean", "f_or_f")
                .withAttribute("Boolean", "f_and_f")
                .withAttribute("Boolean", "f_xor_f")
                .withAttribute("Boolean", "f_implies_f")
                // false vs undefined
                .withAttribute("Boolean", "f_or_u")
                .withAttribute("Boolean", "f_and_u")
                .withAttribute("Boolean", "f_xor_u")
                .withAttribute("Boolean", "f_implies_u")
                // undefined vs true
                .withAttribute("Boolean", "u_or_t")
                .withAttribute("Boolean", "u_and_t")
                .withAttribute("Boolean", "u_xor_t")
                .withAttribute("Boolean", "u_implies_t")
                // undefined vs false
                .withAttribute("Boolean", "u_or_f")
                .withAttribute("Boolean", "u_and_f")
                .withAttribute("Boolean", "u_xor_f")
                .withAttribute("Boolean", "u_implies_f")
                // undefined vs undefined
                .withAttribute("Boolean", "u_or_u")
                .withAttribute("Boolean", "u_and_u")
                .withAttribute("Boolean", "u_xor_u")
                .withAttribute("Boolean", "u_implies_u")

                // not...
                .withAttribute("Boolean", "not_t")
                .withAttribute("Boolean", "not_f")
                .withAttribute("Boolean", "not_u");

        builder.addUnboundOperation("Test")
                .withOutput("Output", cardinality(1,1))
                .withBody("" +
                        "var demo::services::Input i = new demo::services::Input(t = true, f = false)\n" +
                        "var demo::services::Output o = new demo::services::Output()\n" +

                        // true
                        "o.t_or_t = i.t or i.t\n" +
                        "o.t_and_t = i.t and i.t\n" +
                        "o.t_xor_t = i.t xor i.t\n" +
                        "o.t_implies_t = i.t implies i.t\n" +

                        "o.t_or_f= i.t or i.f\n" +
                        "o.t_and_f = i.t and i.f\n" +
                        "o.t_xor_f = i.t xor i.f\n" +
                        "o.t_implies_f = i.t implies i.f\n" +

                        "o.t_or_u = i.t or i.u\n" +
                        "o.t_and_u = i.t and i.u\n" +
                        "o.t_xor_u = i.t xor i.u\n" +
                        "o.t_implies_u = i.t implies i.u\n" +

                        // false
                        "o.f_or_t = i.f or i.t\n" +
                        "o.f_and_t = i.f and i.t\n" +
                        "o.f_xor_t = i.f xor i.t\n" +
                        "o.f_implies_t = i.f implies i.t\n" +

                        "o.f_or_f= i.f or i.f\n" +
                        "o.f_and_f = i.f and i.f\n" +
                        "o.f_xor_f = i.f xor i.f\n" +
                        "o.f_implies_f = i.f implies i.f\n" +

                        "o.f_or_u = i.f or i.u\n" +
                        "o.f_and_u = i.f and i.u\n" +
                        "o.f_xor_u = i.f xor i.u\n" +
                        "o.f_implies_u = i.f implies i.u\n" +

                        // undefined
                        "o.u_or_t = i.u or i.t\n" +
                        "o.u_and_t = i.u and i.t\n" +
                        "o.u_xor_t = i.u xor i.t\n" +
                        "o.u_implies_t = i.u implies i.t\n" +

                        "o.u_or_f= i.u or i.f\n" +
                        "o.u_and_f = i.u and i.f\n" +
                        "o.u_xor_f = i.u xor i.f\n" +
                        "o.u_implies_f = i.u implies i.f\n" +

                        "o.u_or_u = i.u or i.u\n" +
                        "o.u_and_u = i.u and i.u\n" +
                        "o.u_xor_u = i.u xor i.u\n" +
                        "o.u_implies_u = i.u implies i.u\n" +

                        "o.not_f = not i.f\n" +
                        "o.not_t = not i.t\n" +
                        "o.not_u = i.u\n" +

                        "return o");
        fixture.init(builder.build(), datasourceFixture);
        Payload output = run(fixture, "Test", Payload.empty()).getAsPayload(OUTPUT);

        // true
        assertThat(output.get("t_or_t"), is(true));
        assertThat(output.get("t_and_t"), is(true));
        assertThat(output.get("t_xor_t"), is(false));
        assertThat(output.get("t_implies_t"), is(true));

        assertThat(output.get("t_or_f"), is(true));
        assertThat(output.get("t_and_f"), is(false));
        assertThat(output.get("t_xor_f"), is(true));
        assertThat(output.get("t_implies_f"), is(false));

        assertThat(output.get("t_or_u"), is(true));
        assertThat(output.get("t_and_u"), is(nullValue()));
        assertThat(output.get("t_xor_u"), is(nullValue()));
        assertThat(output.get("t_implies_u"), is(nullValue()));
        // false
        assertThat(output.get("f_or_t"), is(true));
        assertThat(output.get("f_and_t"), is(false));
        assertThat(output.get("f_xor_t"), is(true));
        assertThat(output.get("f_implies_t"), is(true));

        assertThat(output.get("f_or_f"), is(false));
        assertThat(output.get("f_and_f"), is(false));
        assertThat(output.get("f_xor_f"), is(false));
        assertThat(output.get("f_implies_f"), is(true));

        assertThat(output.get("f_or_u"), is(nullValue()));
        assertThat(output.get("f_and_u"), is(false));
        assertThat(output.get("f_xor_u"), is(nullValue()));
        assertThat(output.get("f_implies_u"), is(true));
        // undefined
        assertThat(output.get("u_or_t"), is(true));
        assertThat(output.get("u_and_t"), is(nullValue()));
        assertThat(output.get("u_xor_t"), is(nullValue()));
        assertThat(output.get("u_implies_t"), is(true));

        assertThat(output.get("u_or_f"), is(nullValue()));
        assertThat(output.get("u_and_f"), is(false));
        assertThat(output.get("u_xor_f"), is(nullValue()));
        assertThat(output.get("u_implies_f"), is(nullValue()));

        assertThat(output.get("u_or_u"), is(nullValue()));
        assertThat(output.get("u_and_u"), is(nullValue()));
        assertThat(output.get("u_xor_u"), is(nullValue()));
        assertThat(output.get("u_implies_u"), is(nullValue()));

        assertThat(output.get("not_t"), is(false));
        assertThat(output.get("not_f"), is(true));
        assertThat(output.get("not_u"), is(nullValue()));

    }

    @Test
    public void createArray(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Teacher").withAttribute("String", "name");
        modelBuilder.addEntity("TeacherHolder").withRelation("Teacher", "teacher", cardinality(0, 1));
        modelBuilder.addEntity("Course")
                .withAttribute("String", "name")
                .withAggregation("Teacher", "teachers", cardinality(0, -1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::Course course1 = new demo::entities::Course(name = 'course1Name')\n" +
                        "var demo::entities::Course course2 = new demo::entities::Course(name = 'course2Name')\n" +
                        "var demo::entities::Course[] courses = new demo::entities::Course[] { course1, course2 }\n" +
                        "courses += course2\n" +
                        "course1.teachers = new demo::entities::Teacher[] { new demo::entities::Teacher(name = 'teacher1Name') }\n" +
                        "course1.teachers += new demo::entities::Teacher[] { new demo::entities::Teacher(name = 'teacher2Name'), new demo::entities::Teacher(name = 'teacher3Name') }\n" +
                        "var demo::entities::Teacher[] teachers = new demo::entities::Teacher[] { new demo::entities::Teacher(name = 'teacher4Name'), new demo::entities::Teacher(name = 'teacher5Name') }\n" +
                        "course1.teachers += teachers\n" +
                        "var demo::entities::TeacherHolder teacherHolder = new demo::entities::TeacherHolder()\n" +
                        "teacherHolder.teacher = new demo::entities::Teacher(name = 'teacher6Name')\n" +
                        "course1.teachers += teacherHolder.teacher \n" +
                        "var demo::entities::Course[] courses2");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents(DTO + "Course");
        contents.sort(comparing(p -> p.getAs(String.class, "name")));
        assertThat(contents.get(0).getAsCollectionPayload("teachers"), hasSize(6));
        assertThat(contents.get(0).getAsCollectionPayload("teachers").stream().sorted(comparing(p -> p.getAs(String.class, "name"))).collect(toList()).get(5).get("name"), is("teacher6Name"));
    }

    @Test
    public void returnHead(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Course")
                .withAttribute("String", "name");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::Course course1 = new demo::entities::Course(name = 'course1Name')\n" +
                        "var demo::entities::Course course2 = new demo::entities::Course(name = 'course2Name')\n" +
                        "var demo::entities::Course[] courses = new demo::entities::Course[] { course1, course2 }\n" +
                        "var demo::entities::Course result = courses!sort()!head()\n" +
                        "return courses!sort()!head()").withOutput("Course", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("__entityType"), is("demo.entities.Course"));
    }

    @Test
    public void headLambda(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("StringHolder")
                .withAttribute("String", "value");
        modelBuilder.addUnmappedTransferObject("Result")
                .withRelation("StringHolder", "head", cardinality(0, 1))
                .withRelation("StringHolder", "tail", cardinality(0, 1))
                .withRelation("StringHolder", "heads", cardinality(0, -1))
                .withRelation("StringHolder", "tails", cardinality(0, -1));
        modelBuilder.addEntity("Course")
                .withAttribute("String", "name");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::Course course1 = new demo::entities::Course(name = 'course1Name')\n" +
                        "var demo::entities::Course course1b = new demo::entities::Course(name = 'course1Name')\n" +
                        "var demo::entities::Course course2 = new demo::entities::Course(name = 'course2Name')\n" +
                        "var demo::entities::Course course2b = new demo::entities::Course(name = 'course2Name')\n" +
                        "var demo::entities::Course[] courses = new demo::entities::Course[] { course1, course1b, course2, course2b }\n" +
                        "var demo::services::Result result = new demo::services::Result()\n" +
                        "result.head = new demo::services::StringHolder(courses!head(c | c.name DESC).name)\n" +
                        "result.tail = new demo::services::StringHolder(courses!tail(c | c.name DESC).name)\n" +
                        "var demo::entities::Course[] courseHeads = courses!heads(c | c.name DESC)\n" +
                        "for (e in courses!heads(c | c.name DESC)) { result.heads += new demo::services::StringHolder(e.name) }\n" +
                        "return result").withOutput("Result", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload head = run(fixture).getAsPayload(OUTPUT).getAsPayload("head");
        Payload tail = run(fixture).getAsPayload(OUTPUT).getAsPayload("tail");
    }

    @Test
    public void updateMultiForLoop(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Author").withAttribute("String", "name");
        modelBuilder.addMappedTransferObject("AuthorInfo", "Author").withAttribute("String", "name");
        modelBuilder.addEntity("Book")
                .withAttribute("String", "title")
                .withRelation("Author", "authors", cardinality(0, -1));
        modelBuilder.addMappedTransferObject("BookInfo", "Book")
                .withAttribute("String", "title")
                .withAggregation("AuthorInfo", "authors", cardinality(0, -1));
        modelBuilder.addEntity("BookOrder")
                .withRelation("Book", "book", cardinality(0, 1));
        modelBuilder.addMappedTransferObject("BookOrderInfo", "BookOrder")
                .withAggregation("BookInfo", "book", cardinality(0, 1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::services::AuthorInfo[] authors = new demo::services::AuthorInfo[] { new demo::services::AuthorInfo(name = 'author1Name') }\n" +
                        "authors += new demo::services::AuthorInfo('author2Name')\n" +
                        "var demo::services::AuthorInfo author3 = new demo::services::AuthorInfo(name = 'author3Name')\n" +
                        "authors += author3\n" +
                        "var demo::services::BookInfo book = new demo::services::BookInfo(title = 'bookTitle1')\n" +
                        "book.authors += author3\n" +
                        "for (author in authors) {\n" +
                        "book.authors += author\n" +
                        "}\n" +
                        "var demo::services::BookInfo book2 = new demo::services::BookInfo(title = 'bookTitle2')\n" +
                        "var demo::services::AuthorInfo[] authorsFromBook = book.authors\n" +
                        "for (author in book.authors) {\n" +
                        "  book2.authors += author\n" +
                        "}\n" +
                        "var demo::services::BookOrderInfo bookItem = new demo::services::BookOrderInfo(book = book)\n" +
                        "bookItem.book = book2\n" +
                        "bookItem.book.authors += new demo::services::AuthorInfo(name = 'author4Name')\n" +
                        "return bookItem").withOutput("BookOrderInfo", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents("demo.services.BookInfo").stream().sorted(comparing(p -> p.getAs(String.class, "title"))).collect(toList());
        assertThat(contents.get(0).getAsCollectionPayload("authors"), hasSize(3));
        assertThat(contents.get(1).getAsCollectionPayload("authors"), hasSize(4));
    }

    @Test
    public void manyToMany(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Author").withAttribute("String", "name");
        modelBuilder.addEntity("Book").withAttribute("String", "title").withAggregation("Author", "authors", cardinality(0, -1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::Book book1 = new demo::entities::Book(title = 'book1')\n" +
                        "var demo::entities::Book book2 = new demo::entities::Book(title = 'book2')\n" +
                        "var demo::entities::Author author = new demo::entities::Author('pista')\n" +
                        "book1.authors += author\n" +
                        "book2.authors += author");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
        List<Payload> contents = fixture.getContents(DTO + "Book", "authors");
        assertThat(contents.size(), is(4));
        assertThat(contents.get(0).getAsCollectionPayload("authors"), hasSize(1));
        assertThat(contents.get(1).getAsCollectionPayload("authors"), hasSize(1));
    }

    @Test
    public void testReturningInstanceMultipleTimesInResponsePayload(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Author")
                .withAttribute("String", "name");
        modelBuilder.addEntity("Book")
                .withAttribute("String", "title")
                .withAggregation("Author", "authors", cardinality(0, -1));
        modelBuilder.addUnboundOperation("init").withBody(
                "var demo::entities::Author author = new demo::entities::Author(name = 'Gipsz Jakab')\n" +
                        "var demo::entities::Book book1 = new demo::entities::Book(title = 'Book #1')\n" +
                        "var demo::entities::Book book2 = new demo::entities::Book(title = 'Book #2')\n" +
                        "book1.authors += author\n" +
                        "book2.authors += author\n" +
                        "var demo::entities::Book[] allBooks = demo::entities::Book\n" +
                        "return allBooks").withOutput("Book", cardinality(0, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture);
        assertThat(result.getAsCollectionPayload(OUTPUT).size(), is(2));
        Iterator<Payload> bookIterator = result.getAsCollectionPayload(OUTPUT).iterator();
        assertThat(bookIterator.next().getAsCollectionPayload("authors").size(), is(1));
        assertThat(bookIterator.next().getAsCollectionPayload("authors").size(), is(1));
    }

    @Test
    public void refreshPayload(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Attendance")
                .withProperty("Double", "gradePointsAverage", "self.evaluations!avg(eval | eval.gradePoints)")
                .withRelation("Evaluation", "evaluations", cardinality(0, -1));
        modelBuilder.addEntity("Evaluation")
                .withRelation("Attendance", "attendance", cardinality(0, 1))
                .withAttribute("Double", "gradePoints");
        modelBuilder.setTwoWayRelation("Attendance", "evaluations", "Evaluation", "attendance");
        modelBuilder.addUnboundOperation("init",
                "var demo::entities::Attendance attendance = new demo::entities::Attendance()\n" +
                        "var demo::entities::Evaluation evaluation = new demo::entities::Evaluation(gradePoints = 0, attendance = attendance)\n" +
                        "var demo::entities::Evaluation evaluation2 = new demo::entities::Evaluation(gradePoints = 1, attendance = attendance)\n" +
                        "var demo::types::Double averagePoints = attendance.gradePointsAverage\n" +
                        "return attendance", "Attendance");
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("gradePointsAverage"), is(0.5));
        List<Payload> attendances = fixture.getContents("demo._default_transferobjecttypes.entities.Attendance");
        assertThat(attendances.get(0).get("gradePointsAverage"), is(0.5));
        List<Payload> evaluations = fixture.getContents("demo._default_transferobjecttypes.entities.Evaluation");
        assertThat(evaluations.size(), is(2));
    }

    @Test
    public void testReturningNullValues(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("String", "eName");
        modelBuilder.addEntity("F")
                .withAttribute("String", "fName")
                .withAggregation("E", "e", cardinality(0, 1))
                .withAggregation("E", "es", cardinality(0, -1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::E e1 = new demo::entities::E('e1')\n" +
                        "var demo::entities::E e2 = new demo::entities::E()\n" +
                        "var demo::entities::F f = new demo::entities::F()\n" +
                        "f.es += e1\n" +
                        "f.es += e2\n" +
                        "return f").withOutput("F", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
    }

    @Test
    public void unboundAttributeUnique(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E").withAttribute("String", "eName");
        modelBuilder.addMappedTransferObject("F", "E")
                .withAttribute("String", "eName")
                .withAttribute("String", "fName");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::E e1 = new demo::entities::E('e1')\n" +
                        "e1.eName = 'e1'" +
                        "var demo::services::F[] fs1 = demo::services::F\n" +
                        "var demo::services::F f1 = fs1!sort()!head()\n" +
                        "f1.fName = 'f1'" +
                        "return demo::services::F")
                .withOutput("F", cardinality(1, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        assertThat(run(fixture).getAsCollectionPayload(OUTPUT).iterator().next().get("fName"), nullValue());
    }

    @Test
    public void immutableAttributeSet(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("String", "eName");
        modelBuilder.addEntity("F")
                .withAttribute("String", "fName");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::E e = new demo::entities::E('e1')\n" +
                "var immutable demo::entities::E ie = immutable e\n" +
                "e.eName = 'e1_2'\n" +
                "var demo::types::String s = ie.eName\n" +
                "e.eName='e1 updated'\n" +
                "(mutable ie).eName='e1 updated 2'\n" +
                "return (mutable ie)")
                .withOutput("E", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAs(String.class, "eName"), is("e1 updated 2"));
    }

    @Test
    public void immutableReferenceSet(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("String", "eName");
        modelBuilder.addEntity("F")
                .withAggregation("E", "e", cardinality(0, 1))
                .withAttribute("String", "fName");
        modelBuilder.addEntity("G")
                .withAggregation("F", "f", cardinality(0, 1))
                .withAttribute("String", "gName");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::E e = new demo::entities::E('e1')\n" +
                "e.eName = 'e1 updated'\n" +
                "var demo::entities::F f = new demo::entities::F('f1')\n" +
                "var demo::entities::G g = new demo::entities::G('g1')\n" +
                "g.f = f\n" +
                "f.e = e\n" +
                "var immutable demo::entities::G immutableG = immutable g\n" +
                "return mutable immutableG.f.e")
                .withOutput("E", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAs(String.class, "eName"), is("e1 updated"));
    }

    @Test
    public void immutableReferenceAttribute(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("String", "eName");
        modelBuilder.addEntity("F")
                .withAggregation("E", "e", cardinality(0, 1))
                .withAttribute("String", "fName");
        modelBuilder.addEntity("G")
                .withAggregation("F", "f", cardinality(0, 1))
                .withAttribute("String", "gName");
        modelBuilder.addUnmappedTransferObject("StringHolder")
                .withAttribute("String", "value");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::E e = new demo::entities::E('e1')\n" +
                "var demo::entities::F f = new demo::entities::F('f1')\n" +
                "var demo::entities::G g = new demo::entities::G('g1')\n" +
                "g.f = f\n" +
                "f.e = e\n" +
                "var immutable demo::entities::G immutableG = immutable g\n" +
                "e.eName = 'e1 updated'\n" +
                "return new demo::services::StringHolder(immutableG.f.e.eName)")
                .withOutput("StringHolder", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAs(String.class, "value"), is("e1"));
    }

    @Test
    public void immutableCollectionDeclaration(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("D").withAttribute("String", "dName");
        modelBuilder.addEntity("E")
                .withAttribute("String", "eName")
                .withAggregation("D", "ds", cardinality(0, -1));
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::E e1 = new demo::entities::E('e1')\n" +
                "var immutable demo::entities::E e2 = new immutable demo::entities::E('e2')\n" +
                "var demo::entities::E m_e2 = mutable e2\n" +
                "m_e2.eName = 'e2 update'\n" +
                "var immutable demo::entities::D d1 = new immutable demo::entities::D('d1')\n" +
                "m_e2.ds += d1\n" +
                "var demo::entities::E[] es1 = new demo::entities::E[] { e1, mutable e2 }\n" +
                "var immutable demo::entities::E[] ies1 = immutable es1\n" +
                "var immutable demo::entities::E[] ies2 = new immutable demo::entities::E[] { immutable e1, immutable m_e2 }\n" +
                "e1.eName = 'e1 updated'\n" +
                "var immutable demo::entities::D d2 = new immutable demo::entities::D('d2')\n" +
                "e1.ds += d1\n" +
                "e1.ds += d2\n" +
                "var immutable demo::entities::E[] ies3 = immutable demo::entities::E\n" +
                "return mutable ies2"
        ).withOutput("E", cardinality(1, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        List<Payload> output = run(fixture).getAsCollectionPayload(OUTPUT).stream().sorted(comparing(p -> p.getAs(String.class, "eName"))).collect(toList());
        assertThat(output.get(0).getAsCollectionPayload("ds").size(), is(2));
        assertThat(output.get(1).getAsCollectionPayload("ds").iterator().next(), hasEntry("dName", "d1"));

    }

    @Test
    public void immutableReferenceGet(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("D").withAttribute("String", "name");
        modelBuilder.addEntity("E")
                .withAttribute("String", "name")
                .withAggregation("D", "d", cardinality(0, 1));
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::E e = new demo::entities::E('e')\n" +
                "var demo::entities::D d = new demo::entities::D('d')\n" +
                "e.d = d\n" +
                "var immutable demo::entities::E ie = immutable e\n" +
                "var immutable demo::entities::D d2 = immutable ie.d\n" +
                "d.name='d2'\n" +
                "return mutable ie"
        ).withOutput("E", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAsPayload("d"), notNullValue());
        assertThat(output.getAsPayload("d").getAs(String.class, "name"), is("d2"));
    }

    @Test
    public void immutableInput(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("D").withAttribute("String", "name");
        modelBuilder.addEntity("E")
                .withAttribute("String", "name")
                .withAggregation("D", "d", cardinality(0, 1));
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var immutable demo::entities::D d = input.d\n"
        ).withInput("E", "input", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture, "initializer", Payload.map("input", Payload.map("d", Payload.map("__identifier", UUID.randomUUID()))));
    }

    @Test
    public void immutableNavigation(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("D").withAttribute("String", "name");
        modelBuilder.addEntity("E")
                .withAttribute("String", "name")
                .withAggregation("D", "d", cardinality(0, 1));
        modelBuilder
                .addUnboundOperation("createD")
                .withBody("return new demo::entities::D(name='d1')")
                .withOutput("D", cardinality(1, 1));
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var immutable demo::entities::D dImmutable = input.d\n" +
                "var demo::entities::D dMutable = mutable input.d\n" +
                "return dMutable"
        ).withInput("E", "input", cardinality(1, 1)).withOutput("D", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload createDResult = run(fixture, "createD", Payload.empty());
        UUID dId = createDResult.getAsPayload("output").getAs(UUID.class, "__identifier");
        Payload output = run(fixture, "initializer", Payload.map("input", Payload.map("d", Payload.map("__identifier", dId))));
    }

    @Test
    public void immutableDelete(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::E e = new demo::entities::E()\n" +
                "var immutable demo::entities::E ie = immutable e\n" +
                "delete e\n" +
                "return mutable ie")
                .withOutput("E", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output, anEmptyMap());
    }

    @Test
    public void optionalInput(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "return mutable (immutable input)")
                .withInput("E", "input", cardinality(0, 1))
                .withOutput("E", cardinality(0, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture, "initializer", Payload.empty()).getAsPayload(OUTPUT);
        assertThat(output, anEmptyMap());
    }

    @Test
    public void booleanAttribute(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("Boolean", "b");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::E e = new demo::entities::E(b = input.b)\n" +
                "return e")
                .withInput("E", "input", cardinality(1, 1))
                .withOutput("E", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture, "initializer", Payload.map("input", Payload.map("b", true))).getAsPayload(OUTPUT);
        assertThat(output.getAs(Boolean.class, "b"), is(true));
    }

    @Test
    public void objectComparison(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("F");
        modelBuilder.addEntity("E")
                .withRelation("F", "f", cardinality(0, 1));
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("Boolean", "equalE")
                .withAttribute("Boolean", "equalE2")
                .withAttribute("Boolean", "notEqualE")
                .withAttribute("Boolean", "equalF");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::E e = new demo::entities::E()\n" +
                "var demo::entities::E ee = e\n" +
                "var demo::entities::E e2 = new demo::entities::E()\n" +
                "var demo::entities::F f = new demo::entities::F()\n" +
                "var demo::entities::E e3 = new demo::entities::E(f = f)\n" +
                "return new demo::services::Result(equalE = e == ee, equalE2 = e == e2, notEqualE = e != e2, equalF = f == e3.f)")
                .withOutput("Result", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload("output");
        assertThat(output.getAs(Boolean.class, "equalE"), is(true));
        assertThat(output.getAs(Boolean.class, "equalE2"), is(false));
        assertThat(output.getAs(Boolean.class, "notEqualE"), is(true));
        assertThat(output.getAs(Boolean.class, "equalF"), is(true));
    }

    @Test
    public void enumerations(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E").withAttribute("Country", "country");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::types::Countries country = demo::types::Countries#AT\n" +
                "var demo::entities::E e = new demo::entities::E(country = country)\n" +
                "if (e.country == demo::types::Countries#AT) {\n" +
                "e.country = demo::types::Countries#HU\n}\n" +
                "return e")
                .withOutput("E", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAs(Integer.class, "country"), is(0));
    }

    @Test
    public void filter(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E").withAttribute("String", "name");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "new demo::entities::E(name = 'e1')\n" +
                "new demo::entities::E(name = 'e2')\n" +
                "var demo::types::String filteredName = 'e' + '2'\n" +
                "var demo::entities::E[] result = demo::entities::E!filter(e | e.name == filteredName)\n" +
                "return result")
                .withOutput("E", cardinality(0, -1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Collection<Payload> output = run(fixture).getAsCollectionPayload(OUTPUT);
        assertThat(output.iterator().next().get("name"), is("e2"));
    }

    @Test
    public void existsForAll(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E").withAttribute("String", "name");
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("Boolean", "resultExists")
                .withAttribute("Boolean", "resultForAll");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "new demo::entities::E(name = 'e1')\n" +
                "new demo::entities::E(name = 'e2')\n" +
                "var demo::types::String filteredName = 'e' + '2'\n" +
                "var demo::types::Boolean resultExists = demo::entities::E!exists(e | e.name == filteredName)\n" +
                "var demo::types::Boolean resultForAll = demo::entities::E!forAll(e | e.name!first(1) == 'e')\n" +
                "return new demo::services::Result(resultExists = resultExists, resultForAll = resultForAll)")
                .withOutput("Result", cardinality(0, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
    }

    @Test
    public void aggregation(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("String", "name")
                .withAttribute("Integer", "age")
                .withAttribute("Double", "ratio")
                .withAttribute("Date", "date")
                .withAttribute("Timestamp", "time");
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("String", "maxName")
                .withAttribute("String", "minName")
                .withAttribute("Integer", "minAge")
                .withAttribute("Integer", "maxAge")
                .withAttribute("Integer", "sumAge")
                .withAttribute("Double", "minRatio")
                .withAttribute("Double", "maxRatio")
                .withAttribute("Double", "avgAge")
                .withAttribute("Double", "avgRatio")
                .withAttribute("Double", "sumRatio")
                .withAttribute("Date", "minDate")
                .withAttribute("Date", "maxDate")
                .withAttribute("Date", "avgDate")
                .withAttribute("Timestamp", "minTime")
                .withAttribute("Timestamp", "maxTime")
                .withAttribute("Timestamp", "avgTime");

        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "new demo::entities::E(name = 'e1', age=1, ratio=0.1, date=`2000-01-01`, time=`2010-10-10T10:10+00:00`)\n" +
                "new demo::entities::E(name = 'e2', age=2, ratio=0.2, date=`2001-01-01`, time=`2011-10-10T10:10+00:00`)\n" +
                "var demo::types::String maxName = demo::entities::E!max(e | e.name)\n" +
                "return new demo::services::Result(" +
                "maxName = maxName, " +
                "minName = demo::entities::E!min(e | e.name)," +
                "minAge = demo::entities::E!min(e | e.age)," +
                "maxAge = demo::entities::E!max(e | e.age)," +
                "sumAge = demo::entities::E!sum(e | e.age)," +
                "minRatio = demo::entities::E!min(e | e.ratio)," +
                "maxRatio = demo::entities::E!max(e | e.ratio)," +
                "sumRatio = demo::entities::E!sum(e | e.ratio)," +
                "avgRatio = demo::entities::E!avg(e | e.ratio)," +
                "avgAge = demo::entities::E!avg(e | e.age), " +
                "minDate = demo::entities::E!min(e | e.date), " +
                "maxDate = demo::entities::E!max(e | e.date), " +
                "avgDate = demo::entities::E!avg(e | e.date), " +
                "minTime = demo::entities::E!min(e | e.time), " +
                "maxTime = demo::entities::E!max(e | e.time), " +
                "avgTime = demo::entities::E!avg(e | e.time)" +
                ")")
                .withOutput("Result", cardinality(0, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("minName"), is("e1"));
        assertThat(output.get("maxName"), is("e2"));
        assertThat(output.get("minAge"), is(BigInteger.ONE));
        assertThat(output.get("maxAge"), is(BigInteger.valueOf(2)));
        assertThat(output.get("sumAge"), is(BigInteger.valueOf(3)));
        assertThat(output.get("minRatio"), is(BigDecimal.valueOf(0.1)));
        assertThat(output.get("maxRatio"), is(BigDecimal.valueOf(0.2)));
        assertThat(output.get("sumRatio"), is(BigDecimal.valueOf(0.3)));
        assertThat(output.get("avgAge"), is(BigDecimal.valueOf(1.5)));
        assertThat(output.get("avgRatio"), is(BigDecimal.valueOf(0.15)));
    }

    @Test
    public void contains(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E");
        modelBuilder.addEntity("F")
                .withAggregation("E", "es", cardinality(0, -1));
        modelBuilder.addUnmappedTransferObject("U");
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("Boolean", "contains1")
                .withAttribute("Boolean", "contains2")
                .withAttribute("Boolean", "contains3")
                .withAttribute("Boolean", "contains4")
                .withAttribute("Boolean", "contains5")
                .withAttribute("Boolean", "contains6")
                .withAttribute("Boolean", "memberOf1")
                .withAttribute("Boolean", "memberOf2");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::E e1= new demo::entities::E()\n" +
                "var demo::entities::E e2 = new demo::entities::E()\n" +
                "var demo::entities::E[] es = new demo::entities::E[] { e1 }\n" +
                "var demo::entities::F f = new demo::entities::F()\n" +
                "var demo::services::U u = new demo::services::U()\n" +
                "var demo::services::U[] us = new demo::services::U[] { u }\n" +
                "f.es += e1\n" +
                "return new demo::services::Result(" +
                "contains1 = es!contains(e1), " +
                "contains2 = es!contains(e2), " +
                "contains3 = f.es!contains(e1), " +
                "contains4 = f.es!contains(e2), " +
                "contains5 = demo::entities::E!contains(e1), " +
                "contains6 = us!contains(u), " +
                "memberOf1 = e1!memberOf(es) and e1!memberOf(f.es), " +
                "memberOf2 = e2!memberOf(es) or e2!memberOf(f.es))")
                .withOutput("Result", cardinality(0, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("contains1"), is(true));
        assertThat(output.get("contains2"), is(false));
        assertThat(output.get("contains3"), is(true));
        assertThat(output.get("contains4"), is(false));
        assertThat(output.get("contains5"), is(true));
        assertThat(output.get("contains6"), is(true));
        assertThat(output.get("memberOf1"), is(true));
        assertThat(output.get("memberOf2"), is(false));

    }

    @Test
    public void sort(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E").withAttribute("String", "name")
                .withAttribute("Integer", "number");
        modelBuilder.addUnmappedTransferObject("Result")
                .withRelation("E", "sortedEsById", cardinality(0, -1))
                .withRelation("E", "sortedEsByName", cardinality(0, -1))
                .withRelation("E", "sortedEsByNameDesc", cardinality(0, -1))
                .withRelation("E", "sortedEsByNameThenNumber", cardinality(0, -1));
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::E e1= new demo::entities::E(name = 'e1', number=100)\n" +
                "var demo::entities::E e2 = new demo::entities::E(name = 'e2', number=50)\n" +
                "var demo::entities::E e2b = new demo::entities::E(name = 'e2', number=25)\n" +
                "var demo::entities::E[] es = new demo::entities::E[] { e2, e2b, e1 }\n" +
                "var demo::entities::E[] sortedEsById = es!sort()\n" +
                "var demo::entities::E[] sortedEsByName = es!sort(e | e.name)\n" +
                "var demo::entities::E[] sortedEsByNameDesc = es!sort(e | e.name DESC)\n" +
                "var demo::entities::E[] sortedEsByNameThenNumber = es!sort(e | e.name, e.number)\n" +
                "return new demo::services::Result(sortedEsById = sortedEsById, sortedEsByName = sortedEsByName, sortedEsByNameDesc = sortedEsByNameDesc, sortedEsByNameThenNumber = sortedEsByNameThenNumber)")
                .withOutput("Result", cardinality(0, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
    }

    @Test
    public void unmappedStatic(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E").withAttribute("String", "name");
        modelBuilder.addUnmappedTransferObject("Result")
                .withStaticData("Integer", "simpleArithmeticResult", "1+2")
                .withStaticData("Integer", "eCount", "demo::entities::E!count()")
                .withStaticNavigation("E", "eNameA", "demo::entities::E!filter(i | i.name == 'a')!any()", cardinality(0, 1))
                .withStaticNavigation("E", "eNameX", "demo::entities::E!filter(i | i.name == 'x')!any()", cardinality(0, 1))
                .withStaticNavigation("E", "es", "demo::entities::E!sort(i | i.name)", cardinality(0, -1))
        ;
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "new demo::entities::E(name='a');\n" +
                "new demo::entities::E(name='b');" +
                "return new demo::services::Result()")
                .withOutput("Result", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("simpleArithmeticResult"), is(3));
        assertThat(output.get("eCount"), is(2));
    }

    @Test
    public void unmappedStaticAttribute(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E").withAttribute("String", "name");
        modelBuilder.addEntity("F").withRelation("E", "e", cardinality(0, 1));
        modelBuilder.addUnmappedTransferObject("StaticContent")
                .withStaticData("Integer", "simpleArithmeticResult", "1+2")
                .withStaticData("String", "simpleStringResult", "' abc '")
                .withStaticNavigation("E", "eNameA", "demo::entities::E!filter(i | i.name == 'a')!any()", cardinality(0, 1))
                .withStaticNavigation("E", "eNameX", "demo::entities::E!filter(i | i.name == 'x')!any()", cardinality(0, 1))
                .withStaticNavigation("E", "es", "demo::entities::E!sort(i | i.name)", cardinality(0, -1));
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("Integer", "simpleInteger")
                .withAttribute("String", "simpleString")
                .withAttribute("Integer", "eCount")
                .withAttribute("String", "eNameA");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::types::Integer i = demo::services::StaticContent.simpleArithmeticResult + 1\n" +
                "var demo::types::String s = demo::services::StaticContent.simpleStringResult!trim()\n" +
                "var demo::entities::E e = new demo::entities::E(name='a');\n" +
                "new demo::entities::E(name='b');\n" +
                "var demo::entities::F f = new demo::entities::F(e = e)\n" +
                "var demo::types::String eName1 = f.e.name\n" +
                "var demo::types::String eName = demo::services::StaticContent.eNameA.name!upperCase()" +
                "return new demo::services::Result(simpleInteger = i, simpleString = s, eCount = demo::services::StaticContent.es!count(), eNameA = eName)")
                .withOutput("Result", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.get("simpleInteger"), is(BigInteger.valueOf(4)));
        assertThat(output.get("simpleString"), is("abc"));
    }

    @Test
    public void spawn(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Thing").withAttribute("String", "thingName");
        modelBuilder.addEntity("Fruit").withAttribute("String", "fruitName")
                .withSuperType("Thing");
        modelBuilder.addEntity("Apple").withAttribute("String", "name").withSuperType("Fruit");
        modelBuilder.addMappedTransferObject("AppleInfo", "Apple")
                .withAttribute("String", "appleName", "name");
        modelBuilder.addMappedTransferObject("AppleInfo2", "Apple")
                .withAttribute("String", "appleName2", "name");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::Apple e = new demo::entities::Apple(name='apple')\n" +
                "var demo::entities::Fruit f = e\n" +
                "var demo::entities::Apple fa = f as demo::entities::Apple\n" +
                "var demo::entities::Thing thing = fa\n" +
                "(f as demo::entities::Apple).name = 'fapple'\n" +
                "(fa as demo::entities::Apple).name = 'thing_fruit_apple'\n" +
                "var demo::services::AppleInfo ai = e\n" +
                "ai = f as demo::entities::Apple\n" +
                "ai.appleName = ai.appleName + '_'\n" +
                "return ai")
                .withOutput("AppleInfo2", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture, "initializer", Payload.empty());
    }

    @Test
    public void spawnExtended(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();

        modelBuilder.addEntity("Category")
                .withAttribute("String", "categoryName");
        modelBuilder.addMappedTransferObject("CategoryInfo", "Category")
                .withAttribute("String", "categoryName", "categoryName");

        modelBuilder.addEntity("Product")
                .withAttribute("String", "productName")
                .withAttribute("Integer", "unitPrice")
                .withRelation("Category", "category", cardinality(0, 1));
        modelBuilder.addMappedTransferObject("ProductInfo", "Product")
                .withAttribute("String", "productName", "productName");

        modelBuilder.addUnmappedTransferObject("Result")
                .withRelation("Category", "categories", cardinality(0, -1))
                .withRelation("CategoryInfo", "categoryInfos", cardinality(0, -1))
                .withRelation("Product", "products", cardinality(0, -1))
                .withRelation("ProductInfo", "productInfos", cardinality(0, -1));

        modelBuilder.addUnboundOperation("initializer")
                .withOutput("Result", cardinality(1, 1))
                .withBody(
                        "var demo::entities::Product[] products = new demo::entities::Product[] {}\n" +
                        "var demo::services::ProductInfo pi = new demo::services::ProductInfo(productName = 'MousePad')\n" +
                        "\n" +
                        "products += pi\n" +
                        "products!any().unitPrice = 100\n" +
                        "if (products!any().unitPrice == 100) {\n" +
                        "   var demo::services::ProductInfo pi2 = new demo::services::ProductInfo(productName = 'MousePadExtra')\n" +
                        "}\n" +
                        "products -= pi\n" +
                        "\n" +
                        "var demo::services::ProductInfo[] pis = demo::entities::Product\n" +
                        "var demo::entities::Product[] ps = demo::services::ProductInfo\n" +
                        "\n" +
                        "var demo::entities::Product p = new demo::entities::Product(\n" +
                        "	productName = 'Laptop', unitPrice = 500000, \n" +
                        "	category = new demo::entities::Category(categoryName = 'IT')\n" +
                        ")\n" +
                        "ps += p\n" +
                        "ps += new demo::entities::Product(\n" +
                        "	productName = 'TV', unitPrice = 499000, \n" +
                        "	category = new demo::services::CategoryInfo(categoryName = 'HOME')\n" +
                        ")\n" +
                        "ps -= p\n" +
                        "\n" +
                        "return new demo::services::Result(\n" +
                        "   categories = demo::entities::Category,\n" +
                        "   categoryInfos = demo::services::CategoryInfo,\n" +
                        "   products = demo::entities::Product,\n" +
                        "   productInfos = demo::services::ProductInfo\n" +
                        ")\n"
                );
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture, "initializer", Payload.empty()).getAsPayload("output");
        assertNotNull(result);

        // check Category-s
        Collection<Payload> categories = result.getAsCollectionPayload("categories");
        assertNotNull(categories);
        assertEquals(2, categories.size());
        Set<String> categoryNames = categories.stream()
                .map(p -> p.getAs(String.class, "categoryName"))
                .collect(toSet());
        assertEquals(categoryNames, ImmutableSet.of("IT", "HOME"));

        // check CategoryInfo-s
        Collection<Payload> categoryInfos = result.getAsCollectionPayload("categoryInfos");
        assertNotNull(categoryInfos);
        assertEquals(2, categoryInfos.size());
        Set<String> categoryInfoNames = categoryInfos.stream()
                .map(p -> p.getAs(String.class, "categoryName"))
                .collect(toSet());
        assertEquals(categoryInfoNames, ImmutableSet.of("IT", "HOME"));

        // check Products
        Collection<Payload> products = result.getAsCollectionPayload("products");
        assertNotNull(products);
        assertEquals(4, products.size());

        assertProductNameWithUnitPrice(products, "MousePad", 100);
        assertProductNameWithUnitPrice(products, "MousePadExtra", null);
        assertProductNameWithUnitPrice(products, "Laptop", 500000);
        assertProductNameWithUnitPrice(products, "TV", 499000);

        // check ProductInfo-s
        Collection<Payload> productInfos = result.getAsCollectionPayload("productInfos");
        assertNotNull(productInfos);
        assertEquals(4, productInfos.size());

        assertProductNameWithUnitPrice(productInfos, "MousePad", null);
        assertProductNameWithUnitPrice(productInfos, "MousePadExtra", null);
        assertProductNameWithUnitPrice(productInfos, "Laptop", null);
        assertProductNameWithUnitPrice(productInfos, "TV", null);
    }

    private static void assertProductNameWithUnitPrice(Collection<Payload> collection, String productName, Integer expectedUnitPrice) {
        Optional<Payload> targetPayload =
                collection.stream().filter(p -> productName.equals(p.getAs(String.class, "productName"))).findAny();
        assertTrue(targetPayload.isPresent());
        Integer unitPrice = targetPayload.get().getAs(Integer.class, "unitPrice");
        assertEquals(expectedUnitPrice, unitPrice);
    }

    @Test
    public void spawnError() {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Apple").withAttribute("String", "name").withSuperType("Fruit");
        modelBuilder.addMappedTransferObject("AppleInfo", "Apple")
                .withAttribute("String", "appleName", "name");
        modelBuilder.addUnboundOperation("immutableSpawnError").withBody("" +
                "var demo::entities::Apple e = new demo::entities::Apple(name='apple')\n" +
                "var immutable demo::entities::Apple ie = immutable e\n" +
                "var demo::services::AppleInfo mutableIe = ie");
        assertThrows(IllegalArgumentException.class, () -> modelBuilder.build());
    }

    @Test
    public void isAssignable(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Fruit")
                .withAttribute("String", "fruitName");
        modelBuilder.addEntity("Apple")
                .withSuperType("Fruit")
                .withAttribute("String", "appleName");
        modelBuilder.addEntity("Pear")
                .withSuperType("Fruit")
                .withAttribute("String", "pearName");
        modelBuilder.addMappedTransferObject("AppleInfo", "Apple")
                .withAttribute("String", "appleName", "name");
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("String", "appleName")
                .withAttribute("String", "pearName");
        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::entities::Apple apple = new demo::entities::Apple(appleName='apple')\n" +
                "var demo::entities::Fruit[] fruits = new demo::entities::Fruit[] { apple } \n" +
                "var demo::services::AppleInfo[] appleInfos\n" +
                "for (fruit in fruits) {\n" +
                "  if (fruit!isAssignable(demo::entities::Apple)) {\n" +
                "     var demo::entities::Apple app = fruit as demo::entities::Apple\n" +
                "     app.appleName = 'apple2'\n" +
                "   }\n" +
                "}\n" +
                "var demo::entities::Fruit pearFruit = new demo::entities::Pear(pearName='pear')\n" +
                "var demo::entities::Apple appleFromPear = pearFruit as demo::entities::Apple\n" +
                "return new demo::services::Result(appleName = apple.appleName, pearName = appleFromPear.appleName)")
                .withOutput("Result", cardinality(1, 1));
        fixture.init(modelBuilder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
    }

    @Test
    public void actor(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity")
                .withIdentifier("String", "email");
        modelBuilder.addEntity("PhoneEntity")
                .withIdentifier("String", "phone");
        modelBuilder.addMappedTransferObject("EntityInfo", "Entity")
                .withAttribute("String", "email")
                .withAttribute("String", "field");
        modelBuilder.addMappedTransferObject("PhoneEntityInfo", "Entity")
                .withAttribute("String", "phone");
        modelBuilder.addActorType("EntityActor", "EntityInfo", "Entity");
        modelBuilder.addActorType("PhoneActor", "PhoneEntityInfo", "PhoneEntity");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::services::EntityInfo e = new demo::services::EntityInfo(email='test@example.com')\n" +
                        "var demo::services::EntityInfo ap = demo::services::EntityActor!principal()\n" +
                        "var demo::services::PhoneEntityInfo phoneAp = demo::services::PhoneActor!principal()\n" +
                        "if (demo::services::EntityActor!isCurrentActor()) {\n" +
                        "ap.field = 'fieldValue'\n" +
                        "}\n" +
                        "return ap").withOutput("EntityInfo", cardinality(1, 1));
        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload inputPayload = Payload.map(Dispatcher.PRINCIPAL_KEY,
                JudoPrincipal.builder()
                        .name("Test Example")
                        .realm("realm")
                        .client("demo.services.EntityActor")
                        .attributes(Payload.map("email", "test@example.com")).build());
        Payload outputPayload = run(fixture, "initializer", inputPayload).getAsPayload("output");
        assertThat(outputPayload.getAs(String.class, "email"), is("test@example.com"));
        assertThat(outputPayload.getAs(String.class, "__toType"), is("demo.services.EntityInfo"));
        assertThat(outputPayload.getAs(String.class, "field"), is("fieldValue"));
    }

    @Test
    public void timestamp(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("Timestamp", "time");
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("Timestamp", "time")
                .withAttribute("Timestamp", "time2")
                .withAttribute("Timestamp", "time3")
                .withAttribute("Timestamp", "time4")
                .withAttribute("DurationInDays", "diff1")
                .withAttribute("DurationInMinutes", "diff2")
                .withAttribute("Boolean", "comparison");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::E e = new demo::entities::E(time = `2019-07-18T11:11:12+02:00`)\n" +
                        "var demo::types::measured::DurationInDays d1 = 1[day]\n" +
                        "var demo::types::TimeStamp time2 = e.time + 2[day]\n" +
                        "var demo::types::TimeStamp time3 = e.time + d1\n" +
                        "var demo::types::measured::DurationInMinutes m1 = 10[second]\n" +
                        "var demo::types::TimeStamp time4 = e.time - (m1 + 1[minute])\n" + // 70 seconds less than e.time
                        "var demo::types::Boolean b = e.time > `1997-09-12T00:00:00+00:00`\n" +
                        "var demo::types::measured::DurationInDays diff1 = time2!elapsedTimeFrom(e.time)\n" +
                        "var demo::types::measured::DurationInMinutes diff2 = time4!elapsedTimeFrom(time2)\n" + // time4 is 70 seconds earlier than e.time, so time2 is 70sec+2day later than time4
                        "return new demo::services::Result(time = e.time, comparison = b, time2 = time2, time3 = time3, time4 = time4, diff1 = diff1, diff2 = diff2)"
        ).withOutput("Result", cardinality(1, 1));
        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAs(OffsetDateTime.class, "time").atZoneSameInstant(ZoneOffset.systemDefault()),
                is(OffsetDateTime.of(2019, 7, 18, 9, 11, 12, 0, ZoneOffset.ofTotalSeconds(0))
                        .atZoneSameInstant(ZoneOffset.systemDefault())));
        assertThat(output.getAs(OffsetDateTime.class, "time2").atZoneSameInstant(ZoneOffset.systemDefault()),
                is(OffsetDateTime.of(2019, 7, 20, 9, 11, 12, 0, ZoneOffset.ofTotalSeconds(0))
                        .atZoneSameInstant(ZoneOffset.systemDefault())));
        assertThat(output.getAs(OffsetDateTime.class, "time3").atZoneSameInstant(ZoneOffset.systemDefault()),
                is(OffsetDateTime.of(2019, 7, 19, 9, 11, 12, 0, ZoneOffset.ofTotalSeconds(0))
                        .atZoneSameInstant(ZoneOffset.systemDefault())));
        assertThat(output.getAs(OffsetDateTime.class, "time4").atZoneSameInstant(ZoneOffset.systemDefault()),
                is(OffsetDateTime.of(2019, 7, 18, 9, 10, 2, 0, ZoneOffset.ofTotalSeconds(0))
                        .atZoneSameInstant(ZoneOffset.systemDefault())));
        assertThat(output.getAs(Boolean.class, "comparison"), is(true));
        assertThat(output.getAs(BigInteger.class, "diff1"), is(BigInteger.valueOf(2)));
        assertThat(output.getAs(BigInteger.class, "diff2"), is(BigInteger.valueOf(-2881)));
    }

    @Test
    public void date(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("Date", "date");
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("Date", "date")
                .withAttribute("Date", "date1")
                .withAttribute("Date", "date2")
                .withAttribute("Date", "date3")
                .withAttribute("DurationInDays", "diff1")
                .withAttribute("DurationInMinutes", "diff2")
                .withAttribute("Boolean", "comparison");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::E e = new demo::entities::E(date = `2019-07-18`)\n" +
                        "var demo::types::Boolean b = e.date > `1997-09-12`\n" +
                        "var demo::types::measured::DurationInDays d1 = 1[day]\n" +
                        "var demo::types::Date date1 = e.date + 2[day]\n" +
                        "var demo::types::Date date2 = e.date + d1\n" +
                        "var demo::types::Date date3 = date1 - 2880[minute]\n" +
                        "var demo::types::measured::DurationInDays diff1 = date2!elapsedTimeFrom(e.date, demo::measures::Time)\n" +
                        "var demo::types::measured::DurationInMinutes diff2 = e.date!elapsedTimeFrom(date2)\n" +
                        "return new demo::services::Result(date = e.date, comparison = b, date1 = date1, date2 = date2, date3 = date3, diff1 = diff1, diff2 = diff2)"
        ).withOutput("Result", cardinality(1, 1));
        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAs(Boolean.class, "comparison"), is(true));
        assertThat(output.getAs(LocalDate.class, "date"), is(LocalDate.of(2019, 07, 18)));
        assertThat(output.getAs(LocalDate.class, "date1"), is(LocalDate.of(2019, 07, 20)));
        assertThat(output.getAs(LocalDate.class, "date2"), is(LocalDate.of(2019, 07, 19)));
        assertThat(output.getAs(LocalDate.class, "date3"), is(LocalDate.of(2019, 07, 18)));
        assertThat(output.getAs(BigInteger.class, "diff1"), is(BigInteger.valueOf(1)));
        assertThat(output.getAs(BigInteger.class, "diff2"), is(BigInteger.valueOf(-1440)));
    }

    @Test
    public void matches(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("String", "text");
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("Boolean", "match")
                .withAttribute("Boolean", "noMatch");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::E e = new demo::entities::E('abc123')\n" +
                        "var demo::entities::E p = new demo::entities::E('\\\\w+\\\\d+')\n" +
                        "var demo::entities::E p2 = new demo::entities::E('\\\\w{2}\\\\d{2}')\n" +
                        "return new demo::services::Result(match = e.text!matches(p.text), noMatch = e.text!matches(p2.text))"
        ).withOutput("Result", cardinality(1, 1));
        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAs(Boolean.class, "match"), is(true));
        assertThat(output.getAs(Boolean.class, "noMatch"), is(false));
    }

    @Test
    public void like(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("String", "text");
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("Boolean", "match")
                .withAttribute("Boolean", "match2")
                .withAttribute("Boolean", "noMatch");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::E e = new demo::entities::E('abc123')\n" +
                        "var demo::entities::E p = new demo::entities::E('aB%1_3')\n" +
                        "var demo::entities::E p2 = new demo::entities::E('ab_c123')\n" +
                        "return new demo::services::Result(match = e.text!ilike(p.text), match2 = e.text!like(p.text), noMatch = e.text!like(p2.text))"
        ).withOutput("Result", cardinality(1, 1));
        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAs(Boolean.class, "match"), is(true));
        assertThat(output.getAs(Boolean.class, "match2"), is(false));
        assertThat(output.getAs(Boolean.class, "noMatch"), is(false));
    }

    @Test
    public void asString(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("String", "integerAsString")
                .withAttribute("String", "decimalAsString")
                .withAttribute("String", "stringAsString")
                .withAttribute("String", "dateAsString")
                .withAttribute("String", "timestampAsString")
                .withAttribute("String", "booleanAsString")
                .withAttribute("String", "enumAsString");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "return new demo::services::Result(" +
                        "integerAsString = 123!asString(), " +
                        "decimalAsString = 3.1415!asString(), " +
                        "stringAsString = 'apple'!asString(), " +
                        "dateAsString = `2021-02-26`!asString(), " +
                        "timestampAsString = `2019-01-02T03:04:05.678+01:00`!asString(), " +
                        "booleanAsString = true!asString(), " +
                        "enumAsString =  demo::types::Countries#RO!asString()" +
                        ")"
        ).withOutput("Result", cardinality(1, 1));
        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload(OUTPUT);
        assertThat(output.getAs(String.class, "integerAsString"), is("123"));
        assertThat(output.getAs(String.class, "decimalAsString"), is("3.1415"));
        assertThat(output.getAs(String.class, "stringAsString"), is("apple"));
        assertThat(output.getAs(String.class, "dateAsString"), is("2021-02-26"));
        assertThat(output.getAs(String.class, "timestampAsString"), is("2019-01-02T03:04:05.678+01:00"));
        assertThat(output.getAs(String.class, "booleanAsString"), is("true"));
        assertThat(output.getAs(String.class, "enumAsString"), is("RO"));
    }

    @Test
    public void switches(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::types::Integer i = true ? 1 : 0\n" +
                        "var demo::types::Double d = true ? 1.0 : 0.0\n" +
                        "var demo::types::Countries country = false ? demo::types::Countries#AT : demo::types::Countries#HU\n" +
                        "var demo::types::String s = false ? 'a' : 'b'\n" +
                        "var demo::types::Date date = false ? `2020-01-01` : `2020-12-31`\n" +
                        "var demo::types::TimeStamp timestamp = false ? `2020-01-01T12:38:00+00:00` : `2020-12-31T12:38:00+00:00`\n"
        );
        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture);
    }

    @Test
    public void undefinedInput(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("Integer", "nonRequired");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::types::Integer i = input.nonRequired + 1\n" +
                        "var demo::entities::E e = new demo::entities::E()\n" +
                        "e.nonRequired = e.nonRequired + 1" +
                        "return e")
                .withInput("E", "input", cardinality(0, 1))
                .withOutput("E", cardinality(0, 1));
        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture, "initializer", Payload.map("input", Payload.empty()));
    }

    @Test
    public void undefinedInputCollection(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("Integer", "integerAttr");
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::entities::E e = input!any()\n" +
                        "e.integerAttr = 2\n" +
                        "return e")
                .withInput("E", "input", cardinality(0, -1))
                .withOutput("E", cardinality(0, 1));
        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        run(fixture, "initializer", Payload.map("input", Collections.emptyList()));
    }

    @Test
    public void returnEmptyAny(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("E")
                .withAttribute("String", "id");
        modelBuilder.addUnboundOperation("any")
                .withBody("return demo::entities::E!filter(e | e.id == input.id)!any()")
                .withInput("E", "input", cardinality(1, 1))
                .withOutput("E", cardinality(0, 1));
        modelBuilder.addUnboundOperation("headLegacy")
                .withBody("return demo::entities::E!filter(e | e.id == input.id)!head()")
                .withInput("E", "input", cardinality(1, 1))
                .withOutput("E", cardinality(0, 1));
        modelBuilder.addUnboundOperation("head")
                .withBody("return demo::entities::E!filter(e | e.id == input.id)!head(e2 | e2.id DESC)")
                .withInput("E", "input", cardinality(1, 1))
                .withOutput("E", cardinality(0, 1));
        modelBuilder.addUnboundOperation("anyEmptyInput")
                .withBody("return demo::entities::E!filter(e | e.id == input.id)!any()")
                .withInput("E", "input", cardinality(0, 1))
                .withOutput("E", cardinality(0, 1));

        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        Payload runAny = run(fixture, "any", Payload.map("input", Payload.map("id", "a")));
        assertThat(runAny.getAsPayload("output"), anEmptyMap());
        assertThat(run(fixture, "headLegacy", Payload.map("input", Payload.map("id", "a"))).getAsPayload("output"), anEmptyMap());
        assertThat(run(fixture, "head", Payload.map("input", Payload.map("id", "a"))).getAsPayload("output"), anEmptyMap());
        assertThat(run(fixture, "anyEmptyInput", Payload.map("input", Payload.empty())).getAsPayload("output"), anEmptyMap());
        assertThat(run(fixture, "anyEmptyInput", Payload.empty()).getAsPayload("output"), anEmptyMap());
    }

    @Test
    public void testMeasures(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity")
                .withAttribute("MassStoredInKilograms", "massKg")
                .withAttribute("MassStoredInGrams", "massG")
                .withAttribute("MassStoredInKilograms", "massKgFromG")
                .withAttribute("MassIntegerKg", "massIntegerKg")
                .withAttribute("Boolean", "oneKiloEq");
        modelBuilder.addUnmappedTransferObject("Unmapped")
                .withAttribute("MassStoredInKilograms", "massKg")
                .withAttribute("MassStoredInGrams", "massG")
                .withAttribute("MassStoredInKilograms", "massKgFromG")
                .withAttribute("MassIntegerKg", "massIntegerKg")
                .withAttribute("Boolean", "oneKiloEq");
        modelBuilder.addUnmappedTransferObject("Result")
                .withRelation("Unmapped", "unmapped", cardinality(1, 1))
                .withRelation("Entity", "entity", cardinality(1, 1));
        modelBuilder.addUnboundOperation("initializer").withBody(
                "var demo::types::measured::MassStoredInKilograms kg = 2[kg]\n" +
                        "var demo::types::measured::MassStoredInKilograms g = 3[g]\n" +
                        "var demo::types::measured::MassStoredInGrams gGrams = kg + g + 10[dkg]\n" +
                        "var demo::types::measured::MassStoredInKilograms kgFromGram = gGrams + 10[dkg]\n" +
                        "var demo::types::measured::MassStoredInKilograms oneKilo = 1[kg]\n" +
                        "var demo::types::measured::MassStoredInGrams one1000G = 1000[g]\n" +
                        "var demo::types::measured::MassIntegerKg intKilo = 1000[g]\n" +
                        "var demo::types::Boolean equalsOneKilo = oneKilo == one1000G\n" +
                        "var demo::services::Unmapped unmapped = new demo::services::Unmapped(massKg = kg + g + 5[dkg], massG = gGrams, massKgFromG = kgFromGram, oneKiloEq = equalsOneKilo, massIntegerKg = intKilo + 1000[g])\n" +
                        "var demo::entities::Entity entity = new demo::entities::Entity(massKg = kg + g + 5[dkg], massG = gGrams, massKgFromG = kgFromGram, oneKiloEq = equalsOneKilo, massIntegerKg = intKilo + 1000[g])\n" +
                        "unmapped.massKg = unmapped.massKg + 1[g]\n" +
                        "entity.massKg = entity.massKg + 1[g]\n" +
                        "return new demo::services::Result(unmapped = unmapped, entity = entity)")
                .withOutput("Result", cardinality(1, 1));
        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        Payload entity = run(fixture).getAsPayload("output").getAsPayload("entity");
        Payload unmapped = run(fixture).getAsPayload("output").getAsPayload("unmapped");
        assertThat(entity.getAs(Double.class, "massKg"), is(Double.valueOf(2.054)));
        assertThat(entity.getAs(Double.class, "massG"), comparesEqualTo(Double.valueOf(2103)));
        assertThat(entity.getAs(Double.class, "massKgFromG"), is(Double.valueOf(2.203)));
        assertThat(entity.getAs(Boolean.class, "oneKiloEq"), is(true));
        assertThat(entity.getAs(Integer.class, "massIntegerKg"), is(2));
        assertThat(unmapped.getAs(BigDecimal.class, "massKg"), comparesEqualTo(BigDecimal.valueOf(2.054)));
        assertThat(unmapped.getAs(BigDecimal.class, "massG"), comparesEqualTo(BigDecimal.valueOf(2103)));
        assertThat(unmapped.getAs(BigDecimal.class, "massKgFromG"), comparesEqualTo(BigDecimal.valueOf(2.203)));
        assertThat(unmapped.getAs(Boolean.class, "oneKiloEq"), is(true));
        assertThat(unmapped.getAs(BigInteger.class, "massIntegerKg"), is(BigInteger.valueOf(2)));

    }

    @Test
    public void testEnvironmentVariables(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("Result")
                .withAttribute("Integer", "integer")
                .withAttribute("Boolean", "boolean1")
                .withAttribute("Boolean", "boolean1WithVariable")
                .withAttribute("Boolean", "boolean2")
                .withAttribute("String", "string")
                .withAttribute("String", "stringUndefined")
                .withAttribute("Double", "double")
                .withAttribute("Long", "long")
                .withAttribute("MassStoredInKilograms", "mass")
                .withAttribute("Country", "country")
                .withAttribute("Country", "countryUndefined")
                .withAttribute("Gps", "gps")
                .withAttribute("Date", "date")
                .withAttribute("Timestamp", "time")
                .withAttribute("Date", "dateNow")
                .withAttribute("Timestamp", "timeNow")
        ;

        System.setProperty("integer", "1");
        System.setProperty("boolean1", "true");
        System.setProperty("boolean2", "TRUE");
        System.setProperty("string", "foo");
        System.setProperty("double", "3.1415926535");
        System.setProperty("long", "123456789012345678");
        System.setProperty("country", "AT");
        System.setProperty("mass", "42");
        System.setProperty("date", "2020-11-19");
        System.setProperty("time", "2020-11-19T16:38:00+00:00");
        System.setProperty("gps", "47.510746,19.0346693");

        modelBuilder.addUnboundOperation("initializer").withBody("" +
                "var demo::types::String param = 'boolean1'\n" +
                "return new demo::services::Result(" +
                "integer = demo::types::Integer!getVariable('ENVIRONMENT', 'integer') " +
                ", boolean1 = demo::types::Boolean!getVariable('ENVIRONMENT', 'boolean1') " +
                ", boolean1WithVariable = demo::types::Boolean!getVariable('ENVIRONMENT', param) " +
                ", boolean2 = demo::types::Boolean!getVariable('ENVIRONMENT', 'boolean2') " +
                ", string = demo::types::String!getVariable('ENVIRONMENT', 'string') " +
                ", stringUndefined = demo::types::String!getVariable('ENVIRONMENT', 'stringUndefined') " +
                ", double = demo::types::Double!getVariable('ENVIRONMENT', 'double') " +
                ", long = demo::types::Long!getVariable('ENVIRONMENT', 'long') " +
                ", mass = demo::types::measured::MassStoredInKilograms!getVariable('ENVIRONMENT', 'mass') " +
                ", country = demo::types::Countries!getVariable('ENVIRONMENT', 'country') " +
                ", countryUndefined = demo::types::Countries!getVariable('ENVIRONMENT', 'countryUndefined') " +
//                ", gps = demo::types::GPS!getVariable('ENVIRONMENT', 'gps') " +
                ", date = demo::types::Date!getVariable('ENVIRONMENT', 'date') " +
                ", time = demo::types::TimeStamp!getVariable('ENVIRONMENT', 'time')" +
                ", dateNow = demo::types::Date!now()" +
                ", timeNow = demo::types::TimeStamp!now()" +
                ")").withOutput("Result", cardinality(1, 1));

        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture);
    }

    @Test
    public void testObjectCreationWithSeveralParameters(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();

        final int numberOfArguments = 30;

        ScriptTestEntityBuilder entityBuilder = modelBuilder.addEntity("E");
        StringBuilder bodyBuilder = new StringBuilder("return new demo::entities::E(\n");
        for (int i = 1; i <= numberOfArguments; i++) {
            entityBuilder.withAttribute("String", "a" + i);
            bodyBuilder.append("    a").append(i).append(" = 'a").append(i).append("',\n");
        }
        bodyBuilder.deleteCharAt(bodyBuilder.lastIndexOf(","));
        bodyBuilder.append(")");

        modelBuilder.addUnboundOperation("initializer")
                .withOutput("E", cardinality(1, 1))
                .withBody(bodyBuilder.toString());

        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload result = run(fixture).getAsPayload("output");

        for (int i = 1; i <= numberOfArguments; i++) {
            assertEquals("a" + i, result.getAs(String.class, "a" + i));
        }
    }

    @Test
    public void testUnsetCollection(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();

        modelBuilder.addEntity("Item")
                .withAttribute("String", "name");

        modelBuilder.addEntity("Order")
                .withRelation("Item", "items", cardinality(0, -1));
        modelBuilder.addUnmappedTransferObject("TemporaryOrder")
                .withRelation("Item", "items", cardinality(0, -1));

        modelBuilder.addUnmappedTransferObject("Result")
                .withRelation("Item", "orderItems", cardinality(0, -1))
                .withRelation("Item", "temporaryOrderItems", cardinality(0, -1));

        modelBuilder.addUnboundOperation("keepElements")
                .withBody("var demo::entities::Order o = new demo::entities::Order()\n" +
                          "o.items += new demo::entities::Item(name = 'item1')\n" +
                          "o.items += new demo::entities::Item(name = 'item2')\n" +
                          "\n" +
                          "var demo::services::TemporaryOrder to = new demo::services::TemporaryOrder()\n" +
                          "to.items += new demo::entities::Item(name = 'item3')\n" +
                          "to.items += new demo::entities::Item(name = 'item4')\n" +
                          "\n" +
                          "return new demo::services::Result(orderItems = o.items, temporaryOrderItems = to.items)")
                .withOutput("Result", cardinality(1, 1));
        modelBuilder.addUnboundOperation("dropElements")
                .withBody("var demo::entities::Order o = new demo::entities::Order()\n" +
                          "o.items += new demo::entities::Item(name = 'item1')\n" +
                          "o.items += new demo::entities::Item(name = 'item2')\n" +
                          "unset o.items\n" +
                          "\n" +
                          "var demo::services::TemporaryOrder to = new demo::services::TemporaryOrder()\n" +
                          "to.items += new demo::entities::Item(name = 'item3')\n" +
                          "to.items += new demo::entities::Item(name = 'item4')\n" +
                          "unset to.items\n" +
                          "\n" +
                          "return new demo::services::Result(orderItems = o.items, temporaryOrderItems = to.items)")
                .withOutput("Result", cardinality(1, 1));

        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload keptElements = run(fixture, "keepElements", Payload.empty()).getAsPayload("output");
        Payload droppedElements = run(fixture, "dropElements", Payload.empty()).getAsPayload("output");

        Collection<Payload> orderItems = keptElements.getAsCollectionPayload("orderItems");
        assertNotNull(orderItems);
        assertEquals(2, orderItems.size());
        List<String> orderItemsNames = orderItems.stream()
                .map(p -> p.getAs(String.class, "name"))
                .collect(toList());
        assertTrue(orderItemsNames.contains("item1"));
        assertTrue(orderItemsNames.contains("item2"));

        Collection<Payload> temporaryOrderItems = keptElements.getAsCollectionPayload("temporaryOrderItems");
        assertNotNull(temporaryOrderItems);
        assertEquals(2, temporaryOrderItems.size());
        List<String> temporaryOrderItemsNames = temporaryOrderItems.stream()
                .map(p -> p.getAs(String.class, "name"))
                .collect(toList());
        assertTrue(temporaryOrderItemsNames.contains("item3"));
        assertTrue(temporaryOrderItemsNames.contains("item4"));

        assertEquals(0, droppedElements.getAsCollectionPayload("orderItems").size());
        assertEquals(0, droppedElements.getAsCollectionPayload("temporaryOrderItems").size());
    }

    @Test
    public void testUndefinedMutableInputWithObject(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        testUndefinedMutableInput(fixture, datasourceFixture, 1);
    }

    @Test
    public void testUndefinedMutableInputWithCollection(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        testUndefinedMutableInput(fixture, datasourceFixture, -1);
    }

    private void testUndefinedMutableInput(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture, int relationUpperCardinality) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();

        modelBuilder.addEntity("Consumer")
                .withAttribute("String", "name");
        modelBuilder.addEntity("Order")
                .withAttribute("String", "name")
                .withRelation("Consumer", "consumer", cardinality(0, relationUpperCardinality));

        modelBuilder.addUnboundOperation("createOrder")
                .withInput("Consumer", "input", cardinality(0, relationUpperCardinality))
                .withBody("var demo::entities::Order o = new demo::entities::Order(name = 'o1')\n" +
                          "o.consumer = mutable input\n" +
                          "return o")
                .withOutput("Order", cardinality(1, 1));

        Model model = modelBuilder.build();
        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture, "createOrder", Payload.empty()).getAsPayload("output");
        assertNotNull(output);
        assertEquals("o1", output.getAs(String.class, "name"));
    }

    @Test
    public void testTransientRelationsInScript(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        StringType stringType = newStringTypeBuilder().withName("string").withMaxLength(255).build();

        TransferObjectType funding = newTransferObjectTypeBuilder()
                .withName("Funding")
                .withAttributes(
                        newDataMemberBuilder()
                                .withName("name")
                                .withDataType(stringType)
                                .withMemberType(TRANSIENT)
                                .build())
                .build();

        EntityType confirmation = newEntityTypeBuilder().withName("Confirmation").build();
        confirmation.setMapping(newMappingBuilder().withTarget(confirmation).build());

        TransferObjectType confirmationRequest = newTransferObjectTypeBuilder()
                .withName("ConfirmationRequest")
                .withMapping(newMappingBuilder().withTarget(confirmation).build())
                .withRelations(
                        newOneWayRelationMemberBuilder()
                                .withName("funding")
                                .withTarget(funding)
                                .withRelationKind(AGGREGATION)
                                .withMemberType(TRANSIENT)
                                .withLower(0).withUpper(1)
                                .build(),
                        newOneWayRelationMemberBuilder()
                                .withName("fundings")
                                .withTarget(funding)
                                .withRelationKind(AGGREGATION)
                                .withMemberType(TRANSIENT)
                                .withLower(0).withUpper(-1)
                                .build())
                .build();

        EntityType campaign = newEntityTypeBuilder()
                .withName("Campaign")
                .withOperations(
                        newOperationBuilder()
                                .withName("operation")
                                .withBinding("")
                                .withOperationType(INSTANCE)
                                .withOutput(
                                        newParameterBuilder()
                                                .withName("output")
                                                .withTarget(confirmationRequest)
                                                .withLower(0).withUpper(1)
                                                .build())
                                .withBody("var demo::ConfirmationRequest ret = new demo::ConfirmationRequest();\n" +
                                          "ret.funding = new demo::Funding();\n" +
                                          "ret.funding.name = 'itemA';\n" +
                                          "ret.fundings += new demo::Funding('itemB');\n" +
                                          "ret.fundings += new demo::Funding('itemC');\n" +
                                          "return ret;")
                                .build())
                .build();
        campaign.setMapping(newMappingBuilder().withTarget(campaign).build());

        TransferObjectType campaignInfo = newTransferObjectTypeBuilder()
                .withName("campaignInfo")
                .withOperations(newOperationBuilder()
                        .withName("operation")
                        .withOperationType(OperationType.MAPPED)
                        .withOutput(
                                newParameterBuilder()
                                        .withName("output")
                                        .withTarget(confirmationRequest)
                                        .withLower(0).withUpper(1)
                                        .build())
                        .withBinding("operation")
                        .build()
                )
                .build();
        campaignInfo.setMapping(newMappingBuilder().withTarget(campaign).build());

        hu.blackbelt.judo.meta.esm.namespace.Model model = NamespaceBuilders.newModelBuilder()
                .withName("demo")
                .withElements(asList(stringType, funding, confirmation, confirmationRequest, campaign, campaignInfo))
                .build();

        fixture.init(model, datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");
        Payload output = run(fixture).getAsPayload("output");
        assertNotNull(output);

        Payload itemPayload = output.getAsPayload("funding");
        assertNotNull(itemPayload);
        assertEquals("itemA", itemPayload.getAs(String.class, "name"));

        Collection<Payload> items = output.getAsCollectionPayload("fundings");
        assertNotNull(items);
        assertEquals(2, items.size());
        Set<String> itemNames = items.stream().map(p -> p.getAs(String.class, "name")).collect(toSet());
        assertTrue(itemNames.contains("itemB"));
        assertTrue(itemNames.contains("itemC"));
    }

    @Test
    public void testEqualsOperatorWithBooleansInProperties(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();

        builder.addEntity("Tester")
                .withAttribute("Boolean", "F")
                .withAttribute("Boolean", "T")
                .withAttribute("Boolean", "U")
                .withProperty("Boolean", "FeqF", "self.F == self.F")
                .withProperty("Boolean", "FeqT", "self.F == self.T")
                .withProperty("Boolean", "FeqU", "self.F == self.U")
                .withProperty("Boolean", "FneF", "self.F != self.F")
                .withProperty("Boolean", "FneT", "self.F != self.T")
                .withProperty("Boolean", "FneU", "self.F != self.U")
                .withProperty("Boolean", "TeqF", "self.T == self.F")
                .withProperty("Boolean", "TeqT", "self.T == self.T")
                .withProperty("Boolean", "TeqU", "self.T == self.U")
                .withProperty("Boolean", "TneF", "self.T != self.F")
                .withProperty("Boolean", "TneT", "self.T != self.T")
                .withProperty("Boolean", "TneU", "self.T != self.U")
                .withProperty("Boolean", "UeqF", "self.U == self.F")
                .withProperty("Boolean", "UeqT", "self.U == self.T")
                .withProperty("Boolean", "UeqU", "self.U == self.U")
                .withProperty("Boolean", "UneF", "self.U != self.F")
                .withProperty("Boolean", "UneT", "self.U != self.T")
                .withProperty("Boolean", "UneU", "self.U != self.U");

        builder.addUnboundOperation("op")
                .withBody("return new demo::entities::Tester(T = true, F = false)")
                .withOutput("Tester", cardinality(1, 1));

        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");

        Payload result = run(fixture, "op", Payload.empty());
        assertThat(result, notNullValue());
        Payload output = result.getAsPayload(OUTPUT);
        assertThat(output, notNullValue());

        assertThat(output.getAs(Boolean.class, "FeqF"), equalTo(true));
        assertThat(output.getAs(Boolean.class, "FeqT"), equalTo(false));
        assertThat(output.getAs(Boolean.class, "FeqU"), nullValue());
        assertThat(output.getAs(Boolean.class, "FneF"), equalTo(false));
        assertThat(output.getAs(Boolean.class, "FneT"), equalTo(true));
        assertThat(output.getAs(Boolean.class, "FneU"), nullValue());
        assertThat(output.getAs(Boolean.class, "TeqF"), equalTo(false));
        assertThat(output.getAs(Boolean.class, "TeqT"), equalTo(true));
        assertThat(output.getAs(Boolean.class, "TeqU"), nullValue());
        assertThat(output.getAs(Boolean.class, "TneF"), equalTo(true));
        assertThat(output.getAs(Boolean.class, "TneT"), equalTo(false));
        assertThat(output.getAs(Boolean.class, "TneU"), nullValue());
        assertThat(output.getAs(Boolean.class, "UeqF"), nullValue());
        assertThat(output.getAs(Boolean.class, "UeqT"), nullValue());
        assertThat(output.getAs(Boolean.class, "UeqU"), nullValue());
        assertThat(output.getAs(Boolean.class, "UneF"), nullValue());
        assertThat(output.getAs(Boolean.class, "UneT"), nullValue());
        assertThat(output.getAs(Boolean.class, "UneU"), nullValue());
    }

    private static String uppercaseFirstOrUndefined(String s) {
        if (s == null || s.isEmpty()) {
            return "Undefined";
        }
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }

    @Test
    public void testEqualsOperatorWithBooleansInIfBlocks(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder builder = new PsmTestModelBuilder();
        builder.addEntity("Tester")
                .withAttribute("Boolean", "L")
                .withAttribute("Boolean", "R");
        builder.addEntity("BooleanContainer")
                .withAttribute("Boolean", "boolean");

        builder.addBoundOperation("Tester", "operation")
                .withBody("var demo::types::Boolean b\n" +
                          "if (this.L == this.R) {\n" +
                          "    b = true\n" +
                          "} else if (this.L != this.R) {\n" +
                          "    b = false\n" +
                          "}\n" +
                          "return new demo::entities::BooleanContainer(boolean = b)")
                .withOutput("BooleanContainer", cardinality(1, 1));

        Map<String, Boolean> booleanResults = new HashMap<>();
        List<String> booleanValues = List.of("true", "false", "");
        for (String left : booleanValues) {
            for (String right : booleanValues) {
                String operationName = "op" + uppercaseFirstOrUndefined(left) + uppercaseFirstOrUndefined(right);
                builder.addUnboundOperation(operationName)
                        .withInput("Tester", "input", cardinality(1, 1))
                        .withBody("var demo::entities::Tester t = new demo::entities::Tester(L = input.L, R = input.R)\n" +
                                  "return t.operation()")
                        .withOutput("BooleanContainer", cardinality(1, 1));
                booleanResults.put(operationName,
                                   (left.isEmpty() || right.isEmpty())
                                           ? null
                                           : left.equals(right));
            }
        }

        fixture.init(builder.build(), datasourceFixture);
        assertTrue(fixture.isInitialized(), "DAO initialized");

        for (String left : booleanValues) {
            for (String right : booleanValues) {
                String operationName = "op" + uppercaseFirstOrUndefined(left) + uppercaseFirstOrUndefined(right);
                Payload result = run(fixture, operationName, Payload.map("input", Payload.map(
                        "L", left.isEmpty() ? null : Boolean.parseBoolean(left),
                        "R", right.isEmpty() ? null : Boolean.parseBoolean(right)
                )));
                assertThat(result, notNullValue());
                Payload output = result.getAsPayload(OUTPUT);
                assertThat(output, notNullValue());
                log.debug(String.format("Operation %s's result: %s", operationName, output.getAs(Boolean.class, "boolean")));
                assertThat(output.getAs(Boolean.class, "boolean"), equalTo(booleanResults.get(operationName)));
            }
        }
    }

}
