package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.ModelBuilder;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.util.builder.EntityTypeBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.MappingBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.OneWayRelationMemberBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.AGGREGATION;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class RemoveOperationTest {
    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private static final String ENTITY_BASE_FQ_NAME = DTO_PACKAGE + ".Entity_Base";
    private static final String ENTITY_A_FQ_NAME = DTO_PACKAGE + ".Entity_A";
    private static final String ENTITY_A_RELATION_NAME = "entity_A";
    private static final String ENTITY_A_RELATION_FQ_NAME = ENTITY_BASE_FQ_NAME + "#entity_A";

    private static Model MODEL;

    @BeforeAll
    public static void createSpecificEsmModel() {
        final EntityType entityA = EntityTypeBuilder.create()
                .withName("Entity_A")
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Entity_Base")
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName(ENTITY_A_RELATION_NAME)
                                .withTarget(entityA)
                                .withLower(0)
                                .withUpper(-1)
                                .withMemberType(STORED)
                                .withRelationKind(AGGREGATION)
                                .withCreateable(true)
                                .withDeleteable(true)
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());

        MODEL = ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(asList(entityBase, entityA))
                .build();
    }

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    public void testCreateNavigationInstanceAt(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(MODEL, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        final EClass entityBaseEClass = runtimeFixture.getAsmUtils().getClassByFQName(ENTITY_BASE_FQ_NAME)
                .orElseThrow(() -> new RuntimeException(ENTITY_BASE_FQ_NAME + " cannot be found"));
        final EClass entityAEClass = runtimeFixture.getAsmUtils().getClassByFQName(ENTITY_A_FQ_NAME)
                .orElseThrow(() -> new RuntimeException(ENTITY_A_FQ_NAME + " cannot be found"));

        final EReference entityAEReference = runtimeFixture.getAsmUtils().resolveReference(ENTITY_A_RELATION_FQ_NAME)
                .orElseThrow(() -> new RuntimeException(ENTITY_A_RELATION_FQ_NAME + " cannot be found"));

        // init
        final UUID entityBaseEClassID = runtimeFixture.getDao()
                .create(entityBaseEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", entityBaseEClass.getName(), entityBaseEClassID);

        final UUID entityA1EClassID = runtimeFixture.getDao()
                .createNavigationInstanceAt(entityBaseEClassID, entityAEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", entityAEClass.getName(), entityA1EClassID);

        final UUID entityA2EClassID = runtimeFixture.getDao()
                .createNavigationInstanceAt(entityBaseEClassID, entityAEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", entityAEClass.getName(), entityA2EClassID);

        Set<UUID> actualEntityAEReferenceContentIDs = getEReferenceContentIDs(runtimeFixture, entityBaseEClass, entityBaseEClassID, ENTITY_A_RELATION_NAME);
        Set<UUID> expectedEntityAEReferenceContentIDs = ImmutableSet.of(entityA1EClassID, entityA2EClassID);

        assertThat(actualEntityAEReferenceContentIDs, equalTo(expectedEntityAEReferenceContentIDs));

        // remove: before: 1;2 - after: 1
        runtimeFixture.getDao().removeReferences(entityAEReference, entityBaseEClassID, singleton(entityA2EClassID));

        actualEntityAEReferenceContentIDs = getEReferenceContentIDs(runtimeFixture, entityBaseEClass, entityBaseEClassID, ENTITY_A_RELATION_NAME);
        expectedEntityAEReferenceContentIDs = ImmutableSet.of(entityA1EClassID);

        assertThat(actualEntityAEReferenceContentIDs, equalTo(expectedEntityAEReferenceContentIDs));

    }

    @Test
    public void testWithSetReference(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(MODEL, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        final EClass entityBaseEClass = runtimeFixture.getAsmUtils().getClassByFQName(ENTITY_BASE_FQ_NAME)
                .orElseThrow(() -> new RuntimeException(ENTITY_BASE_FQ_NAME + " cannot be found"));
        final EClass entityAEClass = runtimeFixture.getAsmUtils().getClassByFQName(ENTITY_A_FQ_NAME)
                .orElseThrow(() -> new RuntimeException(ENTITY_A_FQ_NAME + " cannot be found"));

        final EReference entityAEReference = runtimeFixture.getAsmUtils().resolveReference(ENTITY_A_RELATION_FQ_NAME)
                .orElseThrow(() -> new RuntimeException(ENTITY_A_RELATION_FQ_NAME + " cannot be found"));

        // init
        final UUID entityBaseEClassID = runtimeFixture.getDao()
                .create(entityBaseEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", entityBaseEClass.getName(), entityBaseEClassID);

        final UUID entityA1EClassID = runtimeFixture.getDao()
                .create(entityAEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", entityAEClass.getName(), entityA1EClassID);

        final UUID entityA2EClassID = runtimeFixture.getDao()
                .create(entityAEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", entityAEClass.getName(), entityA2EClassID);

        runtimeFixture.getDao().setReference(entityAEReference, entityBaseEClassID, asList(entityA1EClassID, entityA2EClassID));

        Set<UUID> actualEntityAEReferenceContentIDs = getEReferenceContentIDs(runtimeFixture, entityBaseEClass, entityBaseEClassID, ENTITY_A_RELATION_NAME);
        Set<UUID> expectedEntityAEReferenceContentIDs = ImmutableSet.of(entityA1EClassID, entityA2EClassID);

        assertThat(actualEntityAEReferenceContentIDs, equalTo(expectedEntityAEReferenceContentIDs));

        // remove: before: 1;2 - after: NA
        runtimeFixture.getDao().removeReferences(entityAEReference, entityBaseEClassID, asList(entityA1EClassID, entityA2EClassID));

        actualEntityAEReferenceContentIDs = getEReferenceContentIDs(runtimeFixture, entityBaseEClass, entityBaseEClassID, ENTITY_A_RELATION_NAME);
        expectedEntityAEReferenceContentIDs = ImmutableSet.of();

        assertThat(actualEntityAEReferenceContentIDs, equalTo(expectedEntityAEReferenceContentIDs));

    }

    private Set<UUID> getEReferenceContentIDs(JudoRuntimeFixture runtimeFixture, EClass ownerEClass, UUID ownerEClassID, String eReferenceName) {
        return runtimeFixture.getDao()
                .getByIdentifier(ownerEClass, ownerEClassID)
                .orElseThrow(() -> new RuntimeException(ownerEClass.getName() + " cannot be found"))
                .getAsCollectionPayload(eReferenceName).stream()
                .map(e -> e.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                .collect(Collectors.toSet());
    }

}
