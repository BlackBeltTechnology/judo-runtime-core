package hu.blackbelt.judo.services.dao;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.NamespaceElement;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.ModelBuilder;
import hu.blackbelt.judo.meta.esm.operation.util.builder.OperationBuilder;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.util.builder.DataMemberBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.EntityTypeBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.MappingBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.OneWayRelationMemberBuilder;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.util.builder.NumericTypeBuilder;
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

import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.operation.OperationType.STATIC;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.DERIVED;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.AGGREGATION;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class CreateThroughDerivedRelationTest {
    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

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
    public void testCreateThroughDerivedRelation(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final NumericType integerType = NumericTypeBuilder.create().withName("integer").withPrecision(3).withScale(0).build();

        final List<NamespaceElement> namespaceElements = new ArrayList<>(singletonList(integerType));

        final EntityType entityA = EntityTypeBuilder.create()
                .withName("A")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("number")
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(integerType)
                                .build())
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());
        namespaceElements.add(entityA);

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName("a")
                                .withTarget(entityA)
                                .withLower(0)
                                .withUpper(-1)
                                .withMemberType(DERIVED)
                                .withRelationKind(AGGREGATION)
                                .withGetterExpression("M::A!filter(e | e.number == 100)")
                                .withCreateable(true)
                                .build())
                .withOperations(
                        OperationBuilder.create()
                                .withName("testOp")
                                .withCustomImplementation(false)
                                .withOperationType(STATIC)
                                .withBinding("")
                                .withStateful(true)
                                .withBody("" +
                                                  "var M::Base b = new M::Base()\n" +
                                                  "b.a += new M::A(number = 50)")
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());
        namespaceElements.add(entityBase);

        final Model model = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        daoFixture.beginTransaction();

        final EClass entityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Base").get();
        final EClass entityAEClass = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".A").get();
        final EReference entityAEReference = daoFixture.getAsmUtils().resolveReference(DTO_PACKAGE + ".Base#a").get();

        final UUID entityBaseID = daoFixture.getDao()
                .create(entityBaseEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        daoFixture.commitTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().createNavigationInstanceAt(entityBaseID, entityAEReference, Payload.map("number", 25), null));
        daoFixture.rollbackTransaction();

        checkEReferenceContentOf(daoFixture, entityBaseEClass, entityBaseID, ImmutableSet.of());
        assertEquals(daoFixture.getDao().getAllOf(entityAEClass).size(), 0);

        daoFixture.beginTransaction();
        final UUID entityAID = daoFixture.getDao()
                .createNavigationInstanceAt(entityBaseID, entityAEReference, Payload.map("number", 100), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        daoFixture.commitTransaction();

        checkEReferenceContentOf(daoFixture, entityBaseEClass, entityBaseID, ImmutableSet.of(entityAID));

        daoFixture.beginTransaction();
        assertThrows(IllegalStateException.class, () -> daoFixture.getOperationImplementations().get("testOp").apply(Payload.empty()));
        daoFixture.rollbackTransaction();

        checkEReferenceContentOf(daoFixture, entityBaseEClass, entityBaseID, ImmutableSet.of(entityAID));
        assertEquals(daoFixture.getDao().getAllOf(entityAEClass).size(), 1);
    }

    private void checkEReferenceContentOf(RdbmsDaoFixture daoFixture, EClass entityBaseEClass, UUID entityBaseID, Set<UUID> expectedEntityAID) {
        final Set<UUID> actualEntityAIDs = daoFixture.getDao()
                .getByIdentifier(entityBaseEClass, entityBaseID)
                .orElseThrow(() -> new RuntimeException(entityBaseEClass.getName() + " with id: " + entityBaseID + " does not exist"))
                .getAsCollectionPayload("a").stream()
                .map(e -> e.getAs(idProviderClass, idProviderName))
                .collect(Collectors.toSet());

        assertThat(actualEntityAIDs, equalTo(expectedEntityAID));
    }

}
