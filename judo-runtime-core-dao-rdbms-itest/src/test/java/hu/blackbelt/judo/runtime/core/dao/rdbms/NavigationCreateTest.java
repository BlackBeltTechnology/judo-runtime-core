package hu.blackbelt.judo.runtime.core.dao.rdbms;


import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.ModelBuilder;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.OneWayRelationMember;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.structure.util.builder.EntityTypeBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.MappingBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.OneWayRelationMemberBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.TransferObjectTypeBuilder;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.structure.MemberType.MAPPED;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.ASSOCIATION;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.COMPOSITION;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class NavigationCreateTest {
    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private static final String ENTITY_BASE_FQ_NAME = DTO_PACKAGE + ".Base";
    private static final String ENTITY_A_FQ_NAME = DTO_PACKAGE + ".A";

    private static final String TRANSFER_ENTITY_BASE_FQ_NAME = MODEL_NAME + ".TO_Base";
    private static final String TRANSFER_ENTITY_A_FQ_NAME = MODEL_NAME + ".TO_A";

    private static final String RELATION_NAME = "a";
    private static final String TRANSFER_RELATION_NAME = "transfer_" + RELATION_NAME;

    private static final String RELATION_FQ_NAME = ENTITY_BASE_FQ_NAME + "#" + RELATION_NAME;
    private static final String TRANSFER_RELATION_FQ_NAME = TRANSFER_ENTITY_BASE_FQ_NAME + "#" + TRANSFER_RELATION_NAME;

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    private static Model getEsmModelWithRelationKind(RelationKind entityBaseToEntityARelationKind) {
        final EntityType entityA = EntityTypeBuilder.create()
                .withName("A")
                .build();
        entityA.setMapping(MappingBuilder.create().withTarget(entityA).build());

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName(RELATION_NAME)
                                .withTarget(entityA)
                                .withLower(0)
                                .withUpper(1)
                                .withMemberType(STORED)
                                .withRelationKind(entityBaseToEntityARelationKind)
                                .withCreateable(true)
                                .build())
                .build();
        entityBase.setMapping(MappingBuilder.create().withTarget(entityBase).build());

        return ModelBuilder.create()
                .withElements(entityBase, entityA)
                .withName(MODEL_NAME)
                .build();
    }

    @Test
    public void testEntity2EntityComposition(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModelWithRelationKind(COMPOSITION), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        test(runtimeFixture);
    }

    @Test
    public void testEntity2EntityAssociation(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModelWithRelationKind(ASSOCIATION), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        test(runtimeFixture);
    }

    private void test(JudoRuntimeFixture runtimeFixture) {
        final EClass entityBaseEClass = runtimeFixture.getAsmUtils().getClassByFQName(ENTITY_BASE_FQ_NAME).get();
        final EClass entityAEClass = runtimeFixture.getAsmUtils().getClassByFQName(ENTITY_A_FQ_NAME).get();
        final EReference entityAEReference = runtimeFixture.getAsmUtils().resolveReference(RELATION_FQ_NAME).get();

        final UUID entityBaseID = runtimeFixture.getDao()
                .create(entityBaseEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", entityBaseEClass.getName(), entityBaseID);

        final UUID entityA1ID = runtimeFixture.getDao()
                .createNavigationInstanceAt(entityBaseID, entityAEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", entityAEClass.getName(), entityA1ID);

        assertThat(runtimeFixture.getDao()
                           .getNavigationResultAt(entityBaseID, entityAEReference).stream()
                           .map(e -> e.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                           .collect(Collectors.toSet()),
                   equalTo(ImmutableSet.of(entityA1ID)));

        assertThrows(IllegalArgumentException.class,
                     () -> runtimeFixture.getDao().createNavigationInstanceAt(entityBaseID, entityAEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                             .mask(Collections.emptyMap())
                             .build()),
                     String.format("Creating another instance of \"%s\" should cause IllegalArgumentException exception" +
                                           " since the [0..1] relation is already full with ID: \"%s\"",
                                   entityAEClass.getName(), entityA1ID));

        // after invalid creation, entityA1ID should be still in the relation content
        assertThat(runtimeFixture.getDao()
                           .getNavigationResultAt(entityBaseID, entityAEReference).stream()
                           .map(e -> e.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                           .collect(Collectors.toSet()),
                   equalTo(ImmutableSet.of(entityA1ID)));

    }

    private static Model getEsmModelWithUpperCardinality(int upper) {
        final EntityType entityA = EntityTypeBuilder.create().withName("A").build();

        final OneWayRelationMember entityARelation = OneWayRelationMemberBuilder.create()
                .withName(RELATION_NAME)
                .withTarget(entityA)
                .withLower(0)
                .withUpper(upper)
                .withMemberType(STORED)
                .withRelationKind(COMPOSITION)
                .withCreateable(true)
                .build();

        final EntityType entityBase = EntityTypeBuilder.create()
                .withName("Base")
                .withRelations(entityARelation)
                .build();

        final TransferObjectType transferEntityA = TransferObjectTypeBuilder.create()
                .withName("TO_A")
                .withMapping(MappingBuilder.create().withTarget(entityA).build())
                .build();

        final TransferObjectType transferEntityBase = TransferObjectTypeBuilder.create()
                .withName("TO_Base")
                .withMapping(MappingBuilder.create().withTarget(entityBase).build())
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName(TRANSFER_RELATION_NAME)
                                .withTarget(transferEntityA)
                                .withBinding(entityARelation)
                                .withLower(entityARelation.getLower())
                                .withUpper(entityARelation.getUpper())
                                .withMemberType(MAPPED)
                                .withRelationKind(ASSOCIATION)
                                .withCreateable(true)
                                .build())
                .build();

        return ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(Arrays.asList(entityA, entityBase, transferEntityA, transferEntityBase))
                .build();
    }

    @Test
    public void testTransferObjectToTransferObjectSingleMappedComposition(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        testCreateThroughMappedComposition(runtimeFixture, datasourceFixture, false);
    }

    @Test
    public void testTransferObjectToTransferObjectCollectionMappedComposition(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        testCreateThroughMappedComposition(runtimeFixture, datasourceFixture, true);
    }

    private void testCreateThroughMappedComposition(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture, boolean isCollection) {
        runtimeFixture.init(getEsmModelWithUpperCardinality(isCollection ? -1 : 1), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        final EClass transferEntityBaseEClass = runtimeFixture.getAsmUtils().getClassByFQName(TRANSFER_ENTITY_BASE_FQ_NAME).get();
        final EClass transferEntityAEClass = runtimeFixture.getAsmUtils().getClassByFQName(TRANSFER_ENTITY_A_FQ_NAME).get();
        final EReference transferEntityAEReference = runtimeFixture.getAsmUtils().resolveReference(TRANSFER_RELATION_FQ_NAME).get();

        final UUID transferEntityBaseID = runtimeFixture.getDao()
                .create(transferEntityBaseEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", transferEntityBaseEClass.getName(), transferEntityBaseID);

        final UUID transferEntityAID = runtimeFixture.getDao()
                .createNavigationInstanceAt(transferEntityBaseID, transferEntityAEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", transferEntityAEClass.getName(), transferEntityAID);

        final Payload navigationResult = runtimeFixture.getDao()
                .getNavigationResultAt(transferEntityBaseID, transferEntityAEReference).get(0);
        final UUID relationContentID = navigationResult.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        assertThat(relationContentID, equalTo(transferEntityAID));
    }

}
