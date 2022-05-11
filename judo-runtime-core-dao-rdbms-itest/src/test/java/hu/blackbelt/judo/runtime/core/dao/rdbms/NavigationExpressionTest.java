package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.NamespaceElement;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.ModelBuilder;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.structure.TwoWayRelationMember;
import hu.blackbelt.judo.meta.esm.structure.util.builder.*;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.util.builder.NumericTypeBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.of;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.DERIVED;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class NavigationExpressionTest {
    private static final String MODEL_NAME = "M";
    private static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private static final NumericType INTEGER = NumericTypeBuilder.create().withName("Integer").withPrecision(3).withScale(0).build();

    private Class<UUID> idProviderClass;
    private String idProviderName;

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture) {
        idProviderClass = daoFixture.getIdProvider().getType();
        idProviderName = daoFixture.getIdProvider().getName();
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testCollectionToObjectNavigationFromSelf(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityA = EntityTypeBuilder.create()
                .withName("A")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build())
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());
        namespaceElements.add(entityA);

        final EntityType entityB = EntityTypeBuilder.create()
                .withName("B")
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName("a")
                                .withMemberType(STORED)
                                .withRelationKind(AGGREGATION)
                                .withLower(1)
                                .withUpper(1)
                                .withCreateable(true)
                                .withTarget(entityA)
                                .build())
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build())
                .build();
        entityB.setMapping(MappingBuilder.create().withTarget(entityB).build());
        namespaceElements.add(entityB);


        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName("bs")
                                .withMemberType(STORED)
                                .withRelationKind(AGGREGATION)
                                .withLower(0)
                                .withUpper(-1)
                                .withCreateable(true)
                                .withTarget(entityB)
                                .build())
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("testAttribute")
                                .withMemberType(DERIVED)
                                .withDataType(INTEGER)
                                .withGetterExpression("self.bs.a!sum(e | e.number)")
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());
        namespaceElements.add(entityBase);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Base").get();

        final Payload entityBasePayload = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("bs",
                        of(Payload.map("number", 1,
                                "a", Payload.map("number", 3)),
                                Payload.map("number", 2,
                                        "a", Payload.map("number", 4)))),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.singletonMap("testAttribute", true))
                                .build());
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBasePayload);

        assertThat(entityBasePayload.getAs(Integer.class, "testAttribute"), equalTo(7));
    }

    @Test
    public void testCollectionToObjectNavigationFromAll(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityA = EntityTypeBuilder.create()
                .withName("A")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build())
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());
        namespaceElements.add(entityA);

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName("a")
                                .withMemberType(STORED)
                                .withRelationKind(AGGREGATION)
                                .withLower(1)
                                .withUpper(1)
                                .withCreateable(true)
                                .withTarget(entityA)
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());
        namespaceElements.add(entityBase);

        final TransferObjectType derivedAttributeCollector = TransferObjectTypeBuilder.create()
                .withName("DerivedAttributeCollector")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("testAttribute")
                                .withMemberType(DERIVED)
                                .withDataType(INTEGER)
                                .withGetterExpression("M::Base.a!sum(e | e.number)")
                                .build())
                .build();
        namespaceElements.add(derivedAttributeCollector);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Base").get();
        final EAttribute testEAttribute = daoFixture.getAsmUtils().resolveAttribute(MODEL_NAME + ".DerivedAttributeCollector#testAttribute").get();

        final Payload entityBase1 = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("a", Payload.map("number", 1)), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build());
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBase1);

        final Payload entityBase2 = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("a", Payload.map("number", 2)), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build());
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBase2);

        final Payload entityBase3 = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("a", Payload.map("number", 3)), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build());
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBase3);

        assertThat(daoFixture.getDao().getStaticData(testEAttribute).getAs(Integer.class, "testAttribute"), equalTo(6));
    }

    @Test
    public void testCollectionToCollectionNavigationFromAll(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityB = EntityTypeBuilder.create()
                .withName("B")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build())
                .build();
        entityB.setMapping(MappingBuilder.create().withTarget(entityB).build());
        namespaceElements.add(entityB);

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName("bs")
                                .withMemberType(STORED)
                                .withRelationKind(AGGREGATION)
                                .withLower(0)
                                .withUpper(-1)
                                .withCreateable(true)
                                .withTarget(entityB)
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());
        namespaceElements.add(entityBase);

        final TransferObjectType derivedAttributeCollector = TransferObjectTypeBuilder.create()
                .withName("DerivedAttributeCollector")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("testAttribute")
                                .withMemberType(DERIVED)
                                .withDataType(INTEGER)
                                .withGetterExpression("M::Base.bs!sum(e | e.number)")
                                .build())
                .build();
        namespaceElements.add(derivedAttributeCollector);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Base").get();
        final EAttribute testEAttribute = daoFixture.getAsmUtils().resolveAttribute(MODEL_NAME + ".DerivedAttributeCollector#testAttribute").get();

        final Payload entityBase1 = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("bs", of(Payload.map("number", 1), Payload.map("number", 2))), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build());
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBase1);

        final Payload entityBase2 = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("bs", of(Payload.map("number", 3), Payload.map("number", 4))), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build());
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBase2);

        final Payload entityBase3 = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("bs", of(Payload.map("number", 5), Payload.map("number", 6))), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build());
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBase3);

        assertThat(daoFixture.getDao().getStaticData(testEAttribute).getAs(Integer.class, "testAttribute"), equalTo(21));
    }

    @Test
    public void testObjectToContainerNavigationFromSelf(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityA = EntityTypeBuilder.create().withName("A").build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());
        namespaceElements.add(entityA);

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName("a")
                                .withMemberType(STORED)
                                .withRelationKind(COMPOSITION)
                                .withLower(1)
                                .withUpper(1)
                                .withCreateable(true)
                                .withTarget(entityA)
                                .build())
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build(),
                        DataMemberBuilder.create()
                                .withName("testAttribute")
                                .withMemberType(DERIVED)
                                .withDataType(INTEGER)
                                .withGetterExpression("self.a!container(M::Base).number")
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());
        namespaceElements.add(entityBase);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Base").get();

        final Payload entityBasePayload = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("a", Payload.empty(),
                        "number", 420), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.singletonMap("testAttribute", true))
                        .build());
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBasePayload);

        assertThat(entityBasePayload.getAs(Integer.class, "testAttribute"), equalTo(420));
    }

    @Test
    public void testObjectToObjectAsTypeFromSelf(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityNativeA = EntityTypeBuilder.create()
                .withName("NativeA")
                .withAbstract_(true)
                .build();
        entityNativeA.setMapping(MappingBuilder.create().withTarget(entityNativeA).build());
        namespaceElements.add(entityNativeA);

        final EntityType entityA = EntityTypeBuilder.create()
                .withName("A")
                .withGeneralizations(GeneralizationBuilder.create().withTarget(entityNativeA).build())
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build())
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());
        namespaceElements.add(entityA);

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName("a")
                                .withMemberType(STORED)
                                .withRelationKind(AGGREGATION)
                                .withLower(1)
                                .withUpper(1)
                                .withCreateable(true)
                                .withTarget(entityNativeA)
                                .build())
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("testAttribute")
                                .withMemberType(DERIVED)
                                .withDataType(INTEGER)
                                .withGetterExpression("self.a!asType(M::A).number")
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());
        namespaceElements.add(entityBase);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Base").get();
        final EClass entityAEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".A").get();

        final UUID entityAID = daoFixture.getDao()
                .create(entityAEClass, Payload.map("number", 314), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", entityAEClass.getName(), entityAID);

        final Payload entityBasePayload = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("a", Payload.map(idProviderName, entityAID)), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.singletonMap("testAttribute", true))
                        .build());
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBasePayload);

        assertThat(entityBasePayload.getAs(Integer.class, "testAttribute"), equalTo(314));
    }

    @Test
    public void testObjectToCollectionAsTypeFromSelf(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityNativeB = EntityTypeBuilder.create()
                .withName("NativeB")
                .withAbstract_(true)
                .build();
        entityNativeB.setMapping(MappingBuilder.create().withTarget(entityNativeB).build());
        namespaceElements.add(entityNativeB);

        final EntityType entityB = EntityTypeBuilder.create()
                .withName("B")
                .withGeneralizations(GeneralizationBuilder.create().withTarget(entityNativeB).build())
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build())
                .build();
        entityB.setMapping(MappingBuilder.create().withTarget(entityB).build());
        namespaceElements.add(entityB);

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName("bs")
                                .withMemberType(STORED)
                                .withRelationKind(AGGREGATION)
                                .withLower(0)
                                .withUpper(-1)
                                .withCreateable(true)
                                .withTarget(entityNativeB)
                                .build())
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("testAttribute")
                                .withMemberType(DERIVED)
                                .withDataType(INTEGER)
                                .withGetterExpression("self.bs!asCollection(M::B)!sum(e | e.number)")
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());
        namespaceElements.add(entityBase);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Base").get();
        final EClass entityBEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".B").get();

        final UUID entityB1ID = daoFixture.getDao()
                .create(entityBEClass, Payload.map("number", 1), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", entityBEClass.getName(), entityB1ID);

        final UUID entityB2ID = daoFixture.getDao()
                .create(entityBEClass, Payload.map("number", 2), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", entityBEClass.getName(), entityB2ID);

        final UUID entityB3ID = daoFixture.getDao()
                .create(entityBEClass, Payload.map("number", 3), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", entityBEClass.getName(), entityB3ID);

        final Payload entityBasePayload = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("bs",
                        of(Payload.map(idProviderName, entityB1ID),
                                Payload.map(idProviderName, entityB2ID),
                                Payload.map(idProviderName, entityB3ID))), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.singletonMap("testAttribute", true))
                        .build());
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBasePayload);

        assertThat(entityBasePayload.getAs(Integer.class, "testAttribute"), equalTo(6));
    }

    @Test
    public void testCollectionAsTypeFromAll(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityNativeA = EntityTypeBuilder.create()
                .withName("NativeA")
                .withAbstract_(true)
                .build();
        entityNativeA.setMapping(MappingBuilder.create().withTarget(entityNativeA).build());
        namespaceElements.add(entityNativeA);

        final EntityType entityA = EntityTypeBuilder.create()
                .withName("A")
                .withGeneralizations(GeneralizationBuilder.create().withTarget(entityNativeA).build())
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build())
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());
        namespaceElements.add(entityA);

        final TransferObjectType derivedAttributeCollector = TransferObjectTypeBuilder.create()
                .withName("DerivedAttributeCollector")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("testAttribute")
                                .withMemberType(DERIVED)
                                .withDataType(INTEGER)
                                .withGetterExpression("M::NativeA!asCollection(M::A)!sum(e | e.number)")
                                .build())
                .build();
        namespaceElements.add(derivedAttributeCollector);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityAEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".A").get();
        final EAttribute testEAttribute = daoFixture.getAsmUtils().resolveAttribute(MODEL_NAME + ".DerivedAttributeCollector#testAttribute").get();

        final UUID entityA1ID = daoFixture.getDao()
                .create(entityAEClass, Payload.map("number", 1), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", entityAEClass.getName(), entityA1ID);

        final UUID entityA2ID = daoFixture.getDao()
                .create(entityAEClass, Payload.map("number", 2), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", entityAEClass.getName(), entityA2ID);

        final UUID entityA3ID = daoFixture.getDao()
                .create(entityAEClass, Payload.map("number", 3), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", entityAEClass.getName(), entityA3ID);

        assertThat(daoFixture.getDao().getStaticData(testEAttribute).getAs(Integer.class, "testAttribute"), equalTo(6));
    }

    @Test
    public void testObjectFilterFromSelf(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .withRequired(true)
                                .build(),
                        DataMemberBuilder.create()
                                .withName("testAttribute")
                                .withMemberType(DERIVED)
                                .withDataType(INTEGER)
                                .withGetterExpression("self!filter(e | e.number == 100).number")
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());
        namespaceElements.add(entityBase);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Base").get();

        final Payload entityBasePayload1 = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("number", 25), null);
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBasePayload1);

        final Payload entityBasePayload2 = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("number", 100), null);
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBasePayload2);

        assertThat(entityBasePayload1.getAs(Integer.class, "testAttribute"), nullValue());
        assertThat(entityBasePayload2.getAs(Integer.class, "testAttribute"), equalTo(100));
    }

    @Test
    public void testObjectToObjectFilterFromSelf(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityA = EntityTypeBuilder.create()
                .withName("A")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build())
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());
        namespaceElements.add(entityA);

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("testAttribute")
                                .withMemberType(DERIVED)
                                .withDataType(INTEGER)
                                .withGetterExpression("self.a!filter(e | e.number == 100).number")
                                .build())
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName("a")
                                .withMemberType(STORED)
                                .withRelationKind(AGGREGATION)
                                .withLower(1)
                                .withUpper(1)
                                .withCreateable(true)
                                .withTarget(entityA)
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());
        namespaceElements.add(entityBase);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Base").get();

        final Payload entityBase1Payload = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("a", Payload.map("number", 25)), null);
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBase1Payload);

        final Payload entityBase2Payload = daoFixture.getDao()
                .create(entityBaseEClass, Payload.map("a", Payload.map("number", 100)), null);
        log.debug("{} created with payload: {}", entityBaseEClass.getName(), entityBase2Payload);

        assertThat(entityBase1Payload.getAs(Integer.class, "testAttribute"), nullValue());
        assertThat(entityBase2Payload.getAs(Integer.class, "testAttribute"), equalTo(100));

    }

    @Test
    public void testNavigatingBetweenTwoWayRelations(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityA = EntityTypeBuilder.create()
                .withName("A")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build(),
                        DataMemberBuilder.create()
                                .withName("testAttribute_from_a")
                                .withDataType(INTEGER)
                                .withMemberType(DERIVED)
                                .withGetterExpression("self.bs.a!sum(e | e.number)")
                                .build(),
                        DataMemberBuilder.create()
                                .withName("testAttribute_from_a_to_b")
                                .withDataType(INTEGER)
                                .withMemberType(DERIVED)
                                .withGetterExpression("self.bs.a.bs!sum(e | e.number)")
                                .build())
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());
        namespaceElements.add(entityA);

        final EntityType entityB = EntityTypeBuilder.create()
                .withName("B")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build(),
                        DataMemberBuilder.create()
                                .withName("testAttribute_from_b")
                                .withDataType(INTEGER)
                                .withMemberType(DERIVED)
                                .withGetterExpression("self.a.bs!sum(e | e.number)")
                                .build(),
                        DataMemberBuilder.create()
                                .withName("testAttribute_from_b_to_a")
                                .withDataType(INTEGER)
                                .withMemberType(DERIVED)
                                .withGetterExpression("self.a.bs.a!sum(e | e.number)")
                                .build())
                .build();
        entityB.setMapping(MappingBuilder.create().withTarget(entityB).build());
        namespaceElements.add(entityB);

        final TwoWayRelationMember toA = TwoWayRelationMemberBuilder.create()
                .withName("a")
                .withTarget(entityA)
                .withLower(0)
                .withUpper(1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();

        final TwoWayRelationMember toB = TwoWayRelationMemberBuilder.create()
                .withName("bs")
                .withTarget(entityB)
                .withLower(0)
                .withUpper(-1)
                .withMemberType(STORED)
                .withRelationKind(AGGREGATION)
                .build();

        toA.setPartner(toB);
        toB.setPartner(toA);

        entityA.getRelations().add(toB);
        entityB.getRelations().add(toA);

        final TransferObjectType derivedAttributeCollector = TransferObjectTypeBuilder.create()
                .withName("DerivedAttributeCollector")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("testAttribute_from_all_b")
                                .withDataType(INTEGER)
                                .withMemberType(DERIVED)
                                .withGetterExpression("M::B.a.bs!sum(e | e.number)")
                                .build(),
                        DataMemberBuilder.create()
                                .withName("testAttribute_from_all_b_filtered")
                                .withDataType(INTEGER)
                                .withMemberType(DERIVED)
                                .withGetterExpression("M::B!filter(b | M::B.a.bs!contains(b))!sum(e | e.number)")
                                .build(),
                        DataMemberBuilder.create()
                                .withName("testAttribute_from_all_a")
                                .withDataType(INTEGER)
                                .withMemberType(DERIVED)
                                .withGetterExpression("M::A.bs.a!sum(e | e.number)")
                                .build())
                .build();
        namespaceElements.add(derivedAttributeCollector);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityAEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".A").get();
        final EClass entityBEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".B").get();

        final Payload entityAPayload = daoFixture.getDao()
                .create(entityAEClass, Payload.map("number", 10,
                        "bs", of(Payload.map("number", 1),
                                Payload.map("number", 2),
                                Payload.map("number", 3))), DAO.QueryCustomizer.<UUID>builder()
                        .mask(ImmutableMap.of(
                                "testAttribute_from_a", true,
                                "testAttribute_from_a_to_b", true))
                        .build());
        log.debug("{} created with payload: {}", entityAEClass.getName(), entityAPayload);

        assertThat(entityAPayload.getAs(Integer.class, "testAttribute_from_a"), equalTo(10));
        assertThat(entityAPayload.getAs(Integer.class, "testAttribute_from_a_to_b"), equalTo(6));

        final List<Payload> allOfEntityB = daoFixture.getDao().getAllOf(entityBEClass);
        final Set<Integer> testAttributeResults1 = allOfEntityB.stream()
                .map(b -> b.getAs(Integer.class, "testAttribute_from_b"))
                .collect(Collectors.toSet());
        final Set<Integer> testAttributeResults2 = allOfEntityB.stream()
                .map(b -> b.getAs(Integer.class, "testAttribute_from_b_to_a"))
                .collect(Collectors.toSet());
        assertThat(testAttributeResults1, equalTo(of(6)));
        assertThat(testAttributeResults2, equalTo(of(10)));

        final EAttribute testEAttribute1 = daoFixture.getAsmUtils().resolveAttribute("M.DerivedAttributeCollector#testAttribute_from_all_a").get();
        final EAttribute testEAttribute2 = daoFixture.getAsmUtils().resolveAttribute("M.DerivedAttributeCollector#testAttribute_from_all_b_filtered").get();
        final EAttribute testEAttribute3 = daoFixture.getAsmUtils().resolveAttribute("M.DerivedAttributeCollector#testAttribute_from_all_b").get();
        assertThat(daoFixture.getDao().getStaticData(testEAttribute1).getAs(Integer.class, "testAttribute_from_all_a"), equalTo(10));
        assertThat(daoFixture.getDao().getStaticData(testEAttribute2).getAs(Integer.class, "testAttribute_from_all_b_filtered"), equalTo(6));
        assertThat(daoFixture.getDao().getStaticData(testEAttribute3).getAs(Integer.class, "testAttribute_from_all_b"), equalTo(6));
    }

    @Test
    public void testTwoWayNavigationLinkedToSort(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Set<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(INTEGER));

        final EntityType entityA = EntityTypeBuilder.create()
                .withName("A")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build())
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());
        namespaceElements.add(entityA);

        final EntityType entityB = EntityTypeBuilder.create()
                .withName("B")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(INTEGER)
                                .build())
                .build();
        entityB.setMapping(MappingBuilder.create().withTarget(entityB).build());
        namespaceElements.add(entityB);

        final TwoWayRelationMember toA = TwoWayRelationMemberBuilder.create()
                .withName("a")
                .withTarget(entityA)
                .withLower(0)
                .withUpper(1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();

        final TwoWayRelationMember toB = TwoWayRelationMemberBuilder.create()
                .withName("bs")
                .withTarget(entityB)
                .withLower(0)
                .withUpper(-1)
                .withMemberType(STORED)
                .withRelationKind(AGGREGATION)
                .build();

        toA.setPartner(toB);
        toB.setPartner(toA);

        entityA.getRelations().add(toB);
        entityB.getRelations().add(toA);

        final TransferObjectType derivedAttributeCollector = TransferObjectTypeBuilder.create()
                .withName("DerivedAttributeCollector")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("testAttribute")
                                .withDataType(INTEGER)
                                .withMemberType(DERIVED)
                                .withGetterExpression("M::A.bs.a!sort(e | e.number desc)!sum(e | e.number)")
                                .build())
                .build();
        namespaceElements.add(derivedAttributeCollector);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        final EClass entityAEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".A").get();

        final Payload entityAPayload = daoFixture.getDao()
                .create(entityAEClass, Payload.map("number", 10,
                        "bs", of(Payload.map("number", 1),
                                Payload.map("number", 2),
                                Payload.map("number", 3))), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build());
        log.debug("{} created with payload: {}", entityAEClass.getName(), entityAPayload);

        final EAttribute testEAttribute = daoFixture.getAsmUtils().resolveAttribute("M.DerivedAttributeCollector#testAttribute").get();
        assertThat(daoFixture.getDao().getStaticData(testEAttribute).getAs(Integer.class, "testAttribute"), equalTo(10));
    }

}
