package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.OneWayRelationMember;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.util.builder.EntityTypeBuilder;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class ContainmentTest {

    public static final String MODEL_NAME = "C";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";
    public static final String REFERENCE_ID = "__referenceId";

    final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {

        Model model = NamespaceBuilders.newModelBuilder().withName(getModelName()).build();
        EntityType container1 = createEntity("Container1");
        EntityType container2 = createEntity("Container2");
        EntityType containment = createEntity("Containment");
        EntityTypeBuilder.use(containment).
                withRelations(
                        newOneWayRelationMemberBuilder()
                                .withName("container1")
                                .withRelationKind(RelationKind.ASSOCIATION)
                                .withTarget(container1)
                                .withLower(1).withUpper(1)
                                .withMemberType(MemberType.DERIVED)
                                .withGetterExpression("self!container(" + getModelName() + "::Container1)")
                                .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("container1Name")
                        .withDataType(stringType)
                        .withRequired(false)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.container1.Container1Name")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("container2")
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withTarget(container2)
                        .withLower(1).withUpper(1)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self!container(" + getModelName() + "::Container2)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("container2Name")
                        .withDataType(stringType)
                        .withRequired(false)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.container2.Container2Name")
                        .build())
                .build();

        EntityType c1 = createEntity("C1", containment);
        EntityType c2 = createEntity("C2", containment);
        EntityType d1_is_c1 = createEntity("D1", c1);
        EntityType d2_is_c1_c2 = createEntity("D2", c1, c2);
        addContainment(container1, containment, 0, 1);
        addContainment(container1, containment, 0, -1);
        addContainment(container1, containment, 0, 1);
        addContainment(container1, containment, 0, -1);
        addContainment(container2, containment, 0, 1);
        addContainment(container2, containment, 0, -1);

        EntityType i1 = createEntity("I1");
        addContainment(c1, i1, 0, 1);
        addContainment(d1_is_c1, i1, 0, -1);

        EntityType related = createEntity("Related");
        EntityTypeBuilder.use(related)
                .withRelations(
                        newOneWayRelationMemberBuilder()
                                .withName("relation")
                                .withRelationKind(RelationKind.ASSOCIATION)
                                .withTarget(containment)
                                .withLower(1).withUpper(1)
                                .withMemberType(MemberType.STORED)
                                .build())
                .withRelations(
                        newOneWayRelationMemberBuilder()
                                .withName("relationD1")
                                .withRelationKind(RelationKind.ASSOCIATION)
                                .withTarget(d1_is_c1)
                                .withLower(0).withUpper(1)
                                .withMemberType(MemberType.STORED)
                                .build())
                .withRelations(
                        newOneWayRelationMemberBuilder()
                                .withName("multiRelation")
                                .withRelationKind(RelationKind.ASSOCIATION)
                                .withTarget(containment)
                                .withLower(0).withUpper(-1)
                                .withMemberType(MemberType.STORED)
                                .build()).build();


        model.getElements().addAll(Arrays.asList(
                stringType, container1, container2, containment, related, c1, c2, d1_is_c1, d2_is_c1_c2, i1
        ));

        return model;
    }

    private void addContainment(EntityType containerType, EntityType containmentType, int lower, int upper) {
        String relationName = containmentType.getName() + (upper != 1 ? "Multi" : "") + "Containment";
        long count = containerType.getRelations().stream().filter(r -> r.getName().matches(relationName + "\\d*")).count();
        String newRelationName = relationName + (count + 1);
        OneWayRelationMember relation = newOneWayRelationMemberBuilder()
                .withName(newRelationName)
                .withRelationKind(RelationKind.COMPOSITION)
                .withLower(lower).withUpper(upper)
                .withMemberType(MemberType.STORED)
                .withTarget(containmentType)
                .build();
        useEntityType(containerType).withRelations(relation).build();
    }

    @NotNull
    private EntityType createEntity(String entityName, EntityType... parents) {
        final EntityType entityType = newEntityTypeBuilder()
                .withName(entityName)
                .withAttributes(newDataMemberBuilder()
                        .withName(entityName + "Name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        if (parents != null) {
            useEntityType(entityType)
                    .withGeneralizations(Arrays.stream(parents).map(e -> newGeneralizationBuilder().withTarget(e).build()).collect(toList())).build();
        }
        entityType.setMapping(newMappingBuilder().withTarget(entityType).build());
        return entityType;
    }

    RdbmsDaoFixture daoFixture;

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        this.daoFixture = daoFixture;
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testContainmentCreatedBeforeContainer(RdbmsDaoFixture daoFixture) {
        Function<String, EClass> getClass = className -> daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + "." + className).get();
        BiFunction<EClass, String, EReference> getReference = (type, referenceName) -> type.getEAllReferences().stream().filter(r -> referenceName.equals(r.getName())).findAny().get();

        EClass container1Type = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Container1").get();
        EClass containmentType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Containment").get();
        EClass c1Type = getClass.apply("C1");
        EClass d1Type = getClass.apply("D1");
        EClass d2Type = getClass.apply("D2");
        EReference containment1Of1Reference = container1Type.getEAllReferences().stream().filter(r -> "ContainmentContainment1".equals(r.getName())).findAny().get();
        final EReference containment1sOf1Reference = container1Type.getEAllReferences().stream().filter(r -> "ContainmentMultiContainment1".equals(r.getName())).findAny().get();
        UUID container1Id = create(daoFixture, container1Type, "Container1");
        UUID containment1Id = create(daoFixture, containmentType, "C1");
        UUID d1Id = create(daoFixture, d1Type, "D1", "ContainmentName", "ContainmentName", "C1Name", "C1Name");
        UUID d2Id = create(daoFixture, d2Type, "D1", "ContainmentName", "ContainmentName", "C1Name", "C1Name", "C2Name", "C2Name");
        UUID containment5Id = create(daoFixture, containmentType, "C5", "C1Name", "C1Name");

        UUID containment2Id = create(daoFixture, containmentType, "C1");
        UUID container2Id = create(daoFixture, container1Type, "Container2", "ContainmentContainment1", daoFixture.getDao().getByIdentifier(getClass.apply("Containment"), containment2Id).get());

        daoFixture.getDao().setReference(containment1Of1Reference, container1Id, Collections.singleton(containment1Id));
        daoFixture.getDao().setReference(containment1sOf1Reference, container1Id, Arrays.asList(containment1Id, containment5Id, d1Id, d2Id));
        daoFixture.getDao().delete(d1Type, d1Id);
        List<Payload> allContainers = daoFixture.getDao().getAllOf(container1Type);
        log.debug("Containers: {}", allContainers);
        log.debug("Child containments: {}", daoFixture.getDao().getAllOf(c1Type));
    }

    @Test
    public void testContainmentInRelation(RdbmsDaoFixture daoFixture) {
        Function<String, EClass> getClass = className -> daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + "." + className).get();
        BiFunction<EClass, String, EReference> getReference = (type, referenceName) -> type.getEAllReferences().stream().filter(r -> referenceName.equals(r.getName())).findAny().get();
        UUID c1Id = create(daoFixture, getClass.apply("C1"), "C1", "ContainmentName", "ContainmentName");
        UUID d1Id = create(daoFixture, getClass.apply("D1"), "C1", "ContainmentName", "ContainmentName", "C1Name", "C1Name");
        UUID d1Id2 = create(daoFixture, getClass.apply("D1"), "D12", "ContainmentName", "ContainmentName", "C1Name", "C1Name");
        UUID d1Id3 = create(daoFixture, getClass.apply("D1"), "D13", "ContainmentName", "ContainmentName", "C1Name", "C1Name");
        UUID relatedId = create(daoFixture, getClass.apply("Related"), "Related", "relation", daoFixture.getDao().getByIdentifier(getClass.apply("D1"), d1Id).get());
        EReference relationRef = getReference.apply(getClass.apply("Related"), "relation");
        daoFixture.getDao().setReference(relationRef, relatedId, Collections.singleton(d1Id));
        EReference relationD1Ref = getReference.apply(getClass.apply("Related"), "relationD1");
        daoFixture.getDao().setReference(relationD1Ref, relatedId, Collections.singleton(d1Id));
        EReference multiRelationRef = getReference.apply(getClass.apply("Related"), "multiRelation");
        daoFixture.getDao().setReference(multiRelationRef, relatedId, Arrays.asList(d1Id2, d1Id3));
        UUID container1Id = create(daoFixture, getClass.apply("Container1"), "Container1");
        EReference containmentReference = getReference.apply(getClass.apply("Container1"), "ContainmentContainment1");
        EReference multiContainmentReference = getReference.apply(getClass.apply("Container1"), "ContainmentMultiContainment1");
        daoFixture.getDao().setReference(containmentReference, container1Id, Collections.singleton(d1Id));
        daoFixture.getDao().setReference(multiContainmentReference, container1Id, Arrays.asList(d1Id2, d1Id3));

        UUID i1Id = create(daoFixture, getClass.apply("I1"), "I1");
        UUID i12Id = create(daoFixture, getClass.apply("I1"), "I12");
        UUID i13Id = create(daoFixture, getClass.apply("I1"), "I13");

        daoFixture.getDao().update(getClass.apply("D1"), Payload.map(
                daoFixture.getIdProvider().getName(), d1Id,
                "D1Name", "D1Name-Updated"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());

        daoFixture.getDao().setReference(getReference.apply(getClass.apply("C1"), "I1Containment1"), c1Id, Arrays.asList(i1Id));
        daoFixture.getDao().setReference(getReference.apply(getClass.apply("D1"), "I1MultiContainment1"), d1Id, Arrays.asList(i12Id, i13Id));
        daoFixture.getDao().setReference(getReference.apply(getClass.apply("D1"), "I1MultiContainment1"), d1Id, Arrays.asList(i12Id, i13Id));

        Payload d1Result = daoFixture.getDao().getNavigationResultAt(relatedId, relationD1Ref).get(0);
        log.debug("Containments as relation: {} ", daoFixture.getDao().getNavigationResultAt(relatedId, relationRef));
        log.debug("D1 containment as relation: {} ", d1Result);
        assertThat(d1Result.getAs(String.class, "D1Name"), is("D1Name-Updated"));
    }

    private UUID create(RdbmsDaoFixture daoFixture, EClass type, String name, Object... customKeyValuePairs) {
        Map<String, Object> payloadMap = new HashMap<>();
        payloadMap.put(type.getName() + "Name", name);
        for (int i = 0; i < customKeyValuePairs.length; i += 2) {
            payloadMap.put((String) customKeyValuePairs[i], customKeyValuePairs[i + 1]);
        }
        Payload container1 = daoFixture.getDao().create(type, Payload.asPayload(payloadMap), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        return container1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
    }

    private UUID createNavigation(RdbmsDaoFixture daoFixture, UUID container1Id, EReference reference, String name) {
        Payload containment = daoFixture.getDao().createNavigationInstanceAt(container1Id, reference, map(
                reference.getEType().getName()+"Name", name,
                REFERENCE_ID, name
                ),
                DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build()
        );
        assertThat(containment.getAs(String.class, REFERENCE_ID), is(name));
        return containment.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
    }

    @Test
    public void testContainment(RdbmsDaoFixture daoFixture) {
        Function<String, EClass> getClass = className -> daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + "." + className).get();
        BiFunction<EClass, String, EReference> getReference = (type, referenceName) -> type.getEAllReferences().stream().filter(r -> referenceName.equals(r.getName())).findAny().get();

        EClass container1Type = getClass.apply("Container1");
        EClass container2Type = getClass.apply("Container2");
        EClass containmentType = getClass.apply("Containment");
        EReference containment1Of1Reference = getReference.apply(container1Type, "ContainmentContainment1");
        EReference containment1sOf1Reference = getReference.apply(container1Type, "ContainmentMultiContainment1");
        EReference containment2Of1Reference = container1Type.getEAllReferences().stream().filter(r -> "ContainmentContainment2".equals(r.getName())).findAny().get();
        EReference containment2sOf1Reference = container1Type.getEAllReferences().stream().filter(r -> "ContainmentMultiContainment2".equals(r.getName())).findAny().get();
        EReference containment1Of2Reference = container2Type.getEAllReferences().stream().filter(r -> "ContainmentContainment1".equals(r.getName())).findAny().get();
        EReference containment1sOf2Reference = container2Type.getEAllReferences().stream().filter(r -> "ContainmentMultiContainment1".equals(r.getName())).findAny().get();
        UUID container1Id = create(daoFixture, container1Type, "Container1", REFERENCE_ID, "Container1");
        UUID container2Id = create(daoFixture, container2Type, "Container2");
        UUID containment1Id = createNavigation(daoFixture, container1Id, containment1Of1Reference, "C1");
        UUID containment5Id = createNavigation(daoFixture, container1Id, containment1sOf1Reference, "C5");
        UUID containment6Id = createNavigation(daoFixture, container1Id, containment1sOf1Reference, "C6");

        UUID containment2Id = createNavigation(daoFixture, container1Id, containment2Of1Reference, "C2");
        UUID containment7Id = createNavigation(daoFixture, container1Id, containment2sOf1Reference, "C7");

        UUID containment3Id = createNavigation(daoFixture, container2Id, containment1Of2Reference, "C3");
        UUID containment8Id = createNavigation(daoFixture, container2Id, containment1sOf2Reference, "C8");
        UUID containment4Id = create(daoFixture, containmentType, "C4");

        log.debug("Running query to get list of containments...");

        final List<Payload> containments = daoFixture.getDao().getAllOf(containmentType);

        log.debug("Containments: {}", containments);

        final Optional<Payload> containment1Loaded = containments.stream().filter(c -> containment1Id.equals(c.getAs(UUID.class, daoFixture.getUuid().getName()))).findAny();
        final Optional<Payload> containment2Loaded = containments.stream().filter(c -> containment2Id.equals(c.getAs(UUID.class, daoFixture.getUuid().getName()))).findAny();
        final Optional<Payload> containment3Loaded = containments.stream().filter(c -> containment3Id.equals(c.getAs(UUID.class, daoFixture.getUuid().getName()))).findAny();
        final Optional<Payload> containment4Loaded = containments.stream().filter(c -> containment4Id.equals(c.getAs(UUID.class, daoFixture.getUuid().getName()))).findAny();
        final Optional<Payload> containment5Loaded = containments.stream().filter(c -> containment5Id.equals(c.getAs(UUID.class, daoFixture.getUuid().getName()))).findAny();
        final Optional<Payload> containment6Loaded = containments.stream().filter(c -> containment6Id.equals(c.getAs(UUID.class, daoFixture.getUuid().getName()))).findAny();
        final Optional<Payload> containment7Loaded = containments.stream().filter(c -> containment7Id.equals(c.getAs(UUID.class, daoFixture.getUuid().getName()))).findAny();
        final Optional<Payload> containment8Loaded = containments.stream().filter(c -> containment8Id.equals(c.getAs(UUID.class, daoFixture.getUuid().getName()))).findAny();

        assertThat(containment1Loaded.get().getAs(String.class, "ContainmentName"), equalTo("C1"));
        assertThat(containment1Loaded.get().getAs(String.class, "container1Name"), equalTo("Container1"));
        assertThat(containment1Loaded.get().getAs(String.class, "container2Name"), nullValue());

        assertThat(containment2Loaded.get().getAs(String.class, "ContainmentName"), equalTo("C2"));
        assertThat(containment2Loaded.get().getAs(String.class, "container1Name"), equalTo("Container1"));
        assertThat(containment2Loaded.get().getAs(String.class, "container2Name"), nullValue());

        assertThat(containment3Loaded.get().getAs(String.class, "ContainmentName"), equalTo("C3"));
        assertThat(containment3Loaded.get().getAs(String.class, "container1Name"), nullValue());
        assertThat(containment3Loaded.get().getAs(String.class, "container2Name"), equalTo("Container2"));

        assertThat(containment4Loaded.get().getAs(String.class, "ContainmentName"), equalTo("C4"));
        assertThat(containment4Loaded.get().getAs(String.class, "container1Name"), nullValue());
        assertThat(containment4Loaded.get().getAs(String.class, "container2Name"), nullValue());

        assertThat(containment5Loaded.get().getAs(String.class, "ContainmentName"), equalTo("C5"));
        assertThat(containment5Loaded.get().getAs(String.class, "container1Name"), equalTo("Container1"));
        assertThat(containment5Loaded.get().getAs(String.class, "container2Name"), nullValue());

        assertThat(containment6Loaded.get().getAs(String.class, "ContainmentName"), equalTo("C6"));
        assertThat(containment6Loaded.get().getAs(String.class, "container1Name"), equalTo("Container1"));
        assertThat(containment6Loaded.get().getAs(String.class, "container2Name"), nullValue());

        assertThat(containment7Loaded.get().getAs(String.class, "ContainmentName"), equalTo("C7"));
        assertThat(containment7Loaded.get().getAs(String.class, "container1Name"), equalTo("Container1"));
        assertThat(containment7Loaded.get().getAs(String.class, "container2Name"), nullValue());

        assertThat(containment8Loaded.get().getAs(String.class, "ContainmentName"), equalTo("C8"));
        assertThat(containment8Loaded.get().getAs(String.class, "container1Name"), nullValue());
        assertThat(containment8Loaded.get().getAs(String.class, "container2Name"), equalTo("Container2"));
    }


}
