package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
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

import java.util.*;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newNumericTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class EmbeddedFilterTest {

    public static final String MODEL_NAME = "EmbeddedFilterTest";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType doubleType = newNumericTypeBuilder().withName("Double").withPrecision(15).withScale(4).build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(getModelName()).build();

        final EntityType product = newEntityTypeBuilder()
                .withName("Product")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withMemberType(MemberType.STORED)
                        .withDataType(stringType)
                        .build())
                .build();
        final EntityType item = newEntityTypeBuilder()
                .withName("Item")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withMemberType(MemberType.STORED)
                        .withDataType(stringType)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("weight")
                        .withMemberType(MemberType.STORED)
                        .withDataType(doubleType)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("product")
                        .withMemberType(MemberType.STORED)
                        .withLower(0).withUpper(1)
                        .withTarget(product)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .build())
                .build();
        useEntityType(item)
                .withMapping(newMappingBuilder().withTarget(item).build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("otherItems")
                        .withMemberType(MemberType.DERIVED)
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withTarget(item)
                        .withGetterExpression("self!container(" + getModelName() + "::Bucket).items!filter(i | i != self)")
                        .build())
                .build();

        final EntityType bucket = newEntityTypeBuilder()
                .withName("Bucket")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("items")
                        .withMemberType(MemberType.STORED)
                        .withTarget(item)
                        .withRelationKind(RelationKind.COMPOSITION)
                        .withLower(0).withUpper(-1)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("mainItem")
                        .withMemberType(MemberType.DERIVED)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withLower(0).withUpper(1)
                        .withGetterExpression("self.items!head(i | i.name)")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("product1Items")
                        .withMemberType(MemberType.DERIVED)
                        .withTarget(item)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withLower(0).withUpper(-1)
                        .withGetterExpression("self.items!filter(i | i.product.name == 'Product1')")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemsHeavierThanBucketAvg")
                        .withMemberType(MemberType.DERIVED)
                        .withTarget(item)
                        .withLower(0).withUpper(-1)
                        .withGetterExpression("self.items!filter(i | i.weight > self.items!avg(x | x.weight))")
                        .withRelationKind(RelationKind.AGGREGATION)
                        .build())
//                .withRelations(newOneWayRelationMemberBuilder()
//                        .withName("itemsHeavierThanBucketAvgOfOthers")
//                        .withMemberType(MemberType.DERIVED)
//                        .withTarget(item)
//                        .withLower(0).withUpper(-1)
//                        .withGetterExpression("self.items!filter(i | i.weight > self.items!filter(x | x != i)!avg(x | x.weight))")
//                        .withRelationKind(RelationKind.AGGREGATION)
//                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("itemsHeavierThanAvg")
                        .withMemberType(MemberType.DERIVED)
                        .withTarget(item)
                        .withLower(0).withUpper(-1)
                        .withGetterExpression("self.items!filter(i | i.weight > " + getModelName() + "::Item!avg(x | x.weight))")
                        .withRelationKind(RelationKind.AGGREGATION)
                        .build())
//                .withRelations(newOneWayRelationMemberBuilder()
//                        .withName("itemsHeavierThanAvgOfOthers")
//                        .withMemberType(MemberType.DERIVED)
//                        .withTarget(item)
//                        .withLower(0).withUpper(-1)
//                        .withGetterExpression("self.items!filter(i | i.weight > " + getModelName() + "::Item!filter(x | x != i)!avg(x | x.weight))")
//                        .withRelationKind(RelationKind.AGGREGATION)
//                        .build())
//                .withRelations(newOneWayRelationMemberBuilder()
//                        .withName("itemsHeavierThanAvgOfOtherBuckets1")
//                        .withMemberType(MemberType.DERIVED)
//                        .withTarget(item)
//                        .withLower(0).withUpper(-1)
//                        .withGetterExpression("self.items!filter(i | i.weight > " + getModelName() + "::Bucket!filter(b | b != self).items!avg(x | x.weight))")
//                        .withRelationKind(RelationKind.AGGREGATION)
//                        .build())
//                .withRelations(newOneWayRelationMemberBuilder()
//                        .withName("itemsHeavierThanAvgOfOtherBuckets2")
//                        .withMemberType(MemberType.DERIVED)
//                        .withTarget(item)
//                        .withLower(0).withUpper(-1)
//                        .withGetterExpression("self.items!filter(i | i.weight > " + getModelName() + "::Bucket!filter(b | b != self)!avg(x | x.items!avg(y | y.weight)))")
//                        .withRelationKind(RelationKind.AGGREGATION)
//                        .build())
                .build();
        useEntityType(bucket).withMapping(newMappingBuilder().withTarget(bucket).build()).build();

        final TransferObjectType tester = newTransferObjectTypeBuilder()
                .withName("Tester")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("bucketsWithProduct1")
                        .withTarget(bucket)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Bucket!filter(b | b.items!exists(i | i.product.name == 'Product1'))")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("bucketsWithMainProduct1")
                        .withTarget(bucket)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Bucket!filter(b | b.mainItem.product.name == 'Product1')")
                        .build())
                .build();

        model.getElements().addAll(Arrays.asList(
                stringType, doubleType,
                bucket, item, product, tester
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
    public void testVariableInEmbeddedQuery(RdbmsDaoFixture daoFixture) {
        final EClass bucketType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Bucket").get();
        final EClass itemType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Item").get();
        final EClass productType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Product").get();
        final EClass testerType = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".Tester").get();
        final EReference otherItemsReference = itemType.getEReferences().stream().filter(r -> "otherItems".equals(r.getName())).findAny().get();
        final EReference bucketsWithProduct1OfTester = testerType.getEReferences().stream().filter(r -> "bucketsWithProduct1".equals(r.getName())).findAny().get();
        final EReference bucketsWithMainProduct1OfTester = testerType.getEReferences().stream().filter(r -> "bucketsWithMainProduct1".equals(r.getName())).findAny().get();

        final Payload product1 = daoFixture.getDao().create(productType, map(
                "name", "Product1"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());

        final Payload bucket1 = daoFixture.getDao().create(bucketType, map(
                "items", Arrays.asList(
                        map(
                                "name", "i1",
                                "weight", 22.0,
                                "product", product1
                        ),
                        map(
                                "name", "i2",
                                "weight", 5.0
                        ),
                        map(
                                "name", "i3",
                                "weight", 15
                        )
                )
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID bucket1Id = bucket1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Payload bucket2 = daoFixture.getDao().create(bucketType, map(
                "items", Arrays.asList(
                        map(
                                "name", "i1",
                                "weight", 20.0
                        ),
                        map(
                                "name", "i2",
                                "weight", 15.0
                        ),
                        map(
                                "name", "i3",
                                "weight", 16.0
                        )
                )
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID bucket2Id = bucket2.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        log.debug("Running query...");

        final Optional<Payload> result1 = daoFixture.getDao().getByIdentifier(bucketType, bucket1Id);
        log.debug("Bucket #1: {}", result1);

        // TODO: add assertion
        assertThat(result1.isPresent(), is(true));
        assertThat(result1.get().getAsCollectionPayload("items"), hasSize(3));
        assertThat(result1.get().getAsCollectionPayload("product1Items"), hasSize(1));
        assertThat(result1.get().getAsCollectionPayload("itemsHeavierThanBucketAvg"), hasSize(2));
        assertThat(result1.get().getAsCollectionPayload("itemsHeavierThanAvg"), hasSize(1));
//        assertThat(result1.get().getAsCollectionPayload("itemsHeavierThanAvgOfOthers1"), hasSize(1));
//        assertThat(result1.get().getAsCollectionPayload("itemsHeavierThanAvgOfOthers2"), hasSize(1));

        final UUID item1Of1Id = result1.get().getAsCollectionPayload("items").iterator().next().getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        final List<Payload> otherItems1 = daoFixture.getDao().getNavigationResultAt(item1Of1Id, otherItemsReference);
        log.debug("Other items of item #1 / bucket #1: {}", otherItems1);

        final Optional<Payload> result2 = daoFixture.getDao().getByIdentifier(bucketType, bucket2Id);
        log.debug("Bucket #2: {}", result2);

        // TODO: add assertion
        assertThat(result2.isPresent(), is(true));
        assertThat(result2.get().getAsCollectionPayload("items"), hasSize(3));
        assertThat(result2.get().getAsCollectionPayload("product1Items"), hasSize(0));
        assertThat(result2.get().getAsCollectionPayload("itemsHeavierThanBucketAvg"), hasSize(1));
        assertThat(result2.get().getAsCollectionPayload("itemsHeavierThanAvg"), hasSize(2));
//        assertThat(result2.get().getAsCollectionPayload("itemsHeavierThanAvgOfOthers1"), hasSize(1));
//        assertThat(result2.get().getAsCollectionPayload("itemsHeavierThanAvgOfOthers2"), hasSize(1));

        final List<Payload> bucketsWithProduct1Result = daoFixture.getDao().getAllReferencedInstancesOf(bucketsWithProduct1OfTester, bucketsWithProduct1OfTester.getEReferenceType());
        log.debug("Buckets with Product1: {}", bucketsWithProduct1Result);
        assertThat(bucketsWithProduct1Result, hasSize(1));

        final List<Payload> bucketsWithMainProduct1Result = daoFixture.getDao().getAllReferencedInstancesOf(bucketsWithMainProduct1OfTester, bucketsWithMainProduct1OfTester.getEReferenceType());
        log.debug("Buckets with main item of Product1: {}", bucketsWithMainProduct1Result);
        assertThat(bucketsWithMainProduct1Result, hasSize(1));
    }
}
