package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.namespace.Model;
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
import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newNumericTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class MaskTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    @AfterEach
    public void teardown(final RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    void testSimpleFilter(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();

        final EntityType entity = newEntityTypeBuilder()
                .withName("Entity")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("upperName")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.name!upperCase()")
                        .build())
                .build();
        final EntityType containment = newEntityTypeBuilder()
                .withName("Containment")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("number")
                        .withDataType(integerType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        final EntityType reference = newEntityTypeBuilder()
                .withName("Reference")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("number")
                        .withDataType(integerType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(entity)
                .withMapping(newMappingBuilder()
                        .withTarget(entity)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("singleContainment")
                        .withTarget(containment)
                        .withLower(0).withUpper(1)
                        .withRelationKind(RelationKind.COMPOSITION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("manyContainment")
                        .withTarget(containment)
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.COMPOSITION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("singleReference")
                        .withTarget(reference)
                        .withLower(0).withUpper(1)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("manyReference")
                        .withTarget(reference)
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("singleContainmentName")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.singleContainment.name")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("singleReferenceName")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.singleReference.name")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("sumManyContainment")
                        .withDataType(integerType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.manyContainment!sum(c | c.number)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("sumManyReference")
                        .withDataType(integerType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.manyReference!sum(r | r.number)")
                        .build())
                .build();

        final TransferObjectType tester = newTransferObjectTypeBuilder()
                .withName("Tester")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("entities")
                        .withLower(0).withUpper(-1)
                        .withTarget(entity)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withGetterExpression(MODEL_NAME + "::Entity")
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(stringType, integerType, entity, containment, reference, tester)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass entityType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".Entity").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EClass referenceType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".Reference").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EClass testerType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (MODEL_NAME + ".Tester").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EReference entitiesReference = testerType.getEAllReferences().stream().filter(r -> "entities".equals(r.getName())).findAny().get();

        final Payload r1 = daoFixture.getDao().create(referenceType, Payload.map(
                "name", "Reference1",
                "number", 100
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final Payload r2 = daoFixture.getDao().create(referenceType, Payload.map(
                "name", "Reference2",
                "number", 200
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final Payload r3 = daoFixture.getDao().create(referenceType, Payload.map(
                "name", "Reference3",
                "number", 300
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final Payload e1 = daoFixture.getDao().create(entityType, Payload.map(
                "name", "Entity1",
                "singleContainment", Payload.map(
                        "name", "SingleContainment",
                        "number", 1
                ),
                "manyContainment", Arrays.asList(
                        Payload.map(
                                "name", "ManyContainment1",
                                "number", 2
                        ),
                        Payload.map(
                                "name", "ManyContainment2",
                                "number", 3
                        )
                ),
                "singleReference", r1,
                "manyReference", Arrays.asList(r2, r3)
        ), null);
        e1.remove("__$created");
        e1.getAsPayload("singleContainment").remove("__$created");
        e1.getAsCollectionPayload("manyContainment").stream().forEach(p -> p.remove("__$created"));

        final UUID e1Id = e1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final List<Payload> result0 = daoFixture.getDao().searchReferencedInstancesOf(entitiesReference, entitiesReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                .build());
        log.debug("Result0: {}", result0);
        assertThat(result0, hasSize(1));
        assertThat(result0.get(0), equalTo(e1));

        final List<Payload> result1 = daoFixture.getDao().searchReferencedInstancesOf(entitiesReference, entitiesReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                .withoutFeatures(true)
                .build());
        log.debug("Result1: {}", result1);
        final Payload expected1 = Payload.asPayload(e1);
        expected1.entrySet().removeIf(e -> !e.getKey().startsWith("__"));
        assertThat(result1, hasSize(1));
        assertThat(result1.get(0), equalTo(expected1));

        final List<Payload> result2 = daoFixture.getDao().searchReferencedInstancesOf(entitiesReference, entitiesReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Result2: {}", result2);
        final Payload expected2 = Payload.asPayload(e1);
        expected2.entrySet().removeIf(e -> !e.getKey().startsWith("__"));
        assertThat(result2, hasSize(1));
        assertThat(result2.get(0), equalTo(expected2));

        final List<Payload> result3 = daoFixture.getDao().searchReferencedInstancesOf(entitiesReference, entitiesReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                .mask(ImmutableMap.of(
                        "name", true,
                        "upperName", true
                ))
                .build());
        log.debug("Result3: {}", result3);
        final Payload expected3 = Payload.asPayload(e1);
        expected3.entrySet().removeIf(e -> !e.getKey().startsWith("__") &&
                !e.getKey().equals("name") &&
                !e.getKey().equals("upperName"));
        assertThat(result3, hasSize(1));
        assertThat(result3.get(0), equalTo(expected3));

        final List<Payload> result4 = daoFixture.getDao().searchReferencedInstancesOf(entitiesReference, entitiesReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                .mask(ImmutableMap.<String, Object>builder()
                        .put("name", true)
                        .put("upperName", true)
                        .put("singleContainment", ImmutableMap.of(
                                "name", true,
                                "number", true
                        ))
                        .put("manyContainment", ImmutableMap.of(
                                "name", true,
                                "number", true
                        ))
                        .put("sumManyContainment", true)
                        .put("sumManyReference", true)
                        .put("singleContainmentName", true)
                        .put("singleReferenceName", true)
                        .build())
                .build());
        log.debug("Result4: {}", result4);
        assertThat(result4, hasSize(1));
        assertThat(result4.get(0), equalTo(e1));

        final List<Payload> result5 = daoFixture.getDao().searchReferencedInstancesOf(entitiesReference, entitiesReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                .mask(ImmutableMap.<String, Object>builder()
                        .put("name", true)
                        .put("singleContainment", Collections.emptyMap())
                        .build())
                .build());
        log.debug("Result5: {}", result5);
        final Payload expected5 = Payload.asPayload(e1);
        expected5.entrySet().removeIf(e -> !e.getKey().startsWith("__") &&
                !e.getKey().equals("name") &&
                !e.getKey().equals("singleContainment"));
        expected5.getAsPayload("singleContainment").entrySet().removeIf(e -> !e.getKey().startsWith("__"));
        assertThat(result5, hasSize(1));
        assertThat(result5.get(0), equalTo(expected5));

        final List<Payload> result6 = daoFixture.getDao().searchReferencedInstancesOf(entitiesReference, entitiesReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                .mask(ImmutableMap.<String, Object>builder()
                        .put("singleContainment", Collections.singletonMap("name", true))
                        .build())
                .build());
        log.debug("Result6: {}", result6);
        final Payload expected6 = Payload.asPayload(e1);
        expected6.entrySet().removeIf(e -> !e.getKey().startsWith("__") &&
                !e.getKey().equals("singleContainment"));
        expected6.getAsPayload("singleContainment").entrySet().removeIf(e -> !e.getKey().startsWith("__") &&
                !e.getKey().equals("name"));
        assertThat(result6, hasSize(1));
        assertThat(result6.get(0), equalTo(expected6));
    }
}
