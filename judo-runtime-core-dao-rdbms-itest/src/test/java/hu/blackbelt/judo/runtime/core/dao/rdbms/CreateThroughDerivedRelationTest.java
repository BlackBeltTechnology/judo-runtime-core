package hu.blackbelt.judo.runtime.core.dao.rdbms;

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
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
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
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class CreateThroughDerivedRelationTest {
    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    public void testCreateThroughDerivedRelation(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
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

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
        runtimeFixture.beginTransaction();

        final EClass entityBaseEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Base").get();
        final EClass entityAEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".A").get();
        final EReference entityAEReference = runtimeFixture.getAsmUtils().resolveReference(DTO_PACKAGE + ".Base#a").get();

        final UUID entityBaseID = runtimeFixture.getDao()
                .create(entityBaseEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        runtimeFixture.commitTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalStateException.class, () -> runtimeFixture.getDao().createNavigationInstanceAt(entityBaseID, entityAEReference, Payload.map("number", 25), null));
        runtimeFixture.rollbackTransaction();

        checkEReferenceContentOf(runtimeFixture, entityBaseEClass, entityBaseID, ImmutableSet.of());
        assertEquals(runtimeFixture.getDao().getAllOf(entityAEClass).size(), 0);

        runtimeFixture.beginTransaction();
        final UUID entityAID = runtimeFixture.getDao()
                .createNavigationInstanceAt(entityBaseID, entityAEReference, Payload.map("number", 100), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        runtimeFixture.commitTransaction();

        checkEReferenceContentOf(runtimeFixture, entityBaseEClass, entityBaseID, ImmutableSet.of(entityAID));

        runtimeFixture.beginTransaction();
        assertThrows(IllegalStateException.class, () -> runtimeFixture.getOperationImplementations().get("testOp").apply(Payload.empty()));
        runtimeFixture.rollbackTransaction();

        checkEReferenceContentOf(runtimeFixture, entityBaseEClass, entityBaseID, ImmutableSet.of(entityAID));
        assertEquals(runtimeFixture.getDao().getAllOf(entityAEClass).size(), 1);
    }

    private void checkEReferenceContentOf(JudoRuntimeFixture runtimeFixture, EClass entityBaseEClass, UUID entityBaseID, Set<UUID> expectedEntityAID) {
        final Set<UUID> actualEntityAIDs = runtimeFixture.getDao()
                .getByIdentifier(entityBaseEClass, entityBaseID)
                .orElseThrow(() -> new RuntimeException(entityBaseEClass.getName() + " with id: " + entityBaseID + " does not exist"))
                .getAsCollectionPayload("a").stream()
                .map(e -> e.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                .collect(Collectors.toSet());

        assertThat(actualEntityAIDs, equalTo(expectedEntityAID));
    }

}
