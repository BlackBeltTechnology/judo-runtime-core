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
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
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
    public void testEntity2EntityComposition(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModelWithRelationKind(COMPOSITION), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        test(daoFixture);
    }

    @Test
    public void testEntity2EntityAssociation(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModelWithRelationKind(ASSOCIATION), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        test(daoFixture);
    }

    private void test(RdbmsDaoFixture daoFixture) {
        final EClass entityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(ENTITY_BASE_FQ_NAME).get();
        final EClass entityAEClass = daoFixture.getAsmUtils().getClassByFQName(ENTITY_A_FQ_NAME).get();
        final EReference entityAEReference = daoFixture.getAsmUtils().resolveReference(RELATION_FQ_NAME).get();

        final UUID entityBaseID = daoFixture.getDao()
                .create(entityBaseEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", entityBaseEClass.getName(), entityBaseID);

        final UUID entityA1ID = daoFixture.getDao()
                .createNavigationInstanceAt(entityBaseID, entityAEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", entityAEClass.getName(), entityA1ID);

        assertThat(daoFixture.getDao()
                           .getNavigationResultAt(entityBaseID, entityAEReference).stream()
                           .map(e -> e.getAs(idProviderClass, idProviderName))
                           .collect(Collectors.toSet()),
                   equalTo(ImmutableSet.of(entityA1ID)));

        assertThrows(IllegalArgumentException.class,
                     () -> daoFixture.getDao().createNavigationInstanceAt(entityBaseID, entityAEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                             .mask(Collections.emptyMap())
                             .build()),
                     String.format("Creating another instance of \"%s\" should cause IllegalArgumentException exception" +
                                           " since the [0..1] relation is already full with ID: \"%s\"",
                                   entityAEClass.getName(), entityA1ID));

        // after invalid creation, entityA1ID should be still in the relation content
        assertThat(daoFixture.getDao()
                           .getNavigationResultAt(entityBaseID, entityAEReference).stream()
                           .map(e -> e.getAs(idProviderClass, idProviderName))
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
    public void testTransferObjectToTransferObjectSingleMappedComposition(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        testCreateThroughMappedComposition(daoFixture, datasourceFixture, false);
    }

    @Test
    public void testTransferObjectToTransferObjectCollectionMappedComposition(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        testCreateThroughMappedComposition(daoFixture, datasourceFixture, true);
    }

    private void testCreateThroughMappedComposition(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture, boolean isCollection) {
        daoFixture.init(getEsmModelWithUpperCardinality(isCollection ? -1 : 1), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass transferEntityBaseEClass = daoFixture.getAsmUtils().getClassByFQName(TRANSFER_ENTITY_BASE_FQ_NAME).get();
        final EClass transferEntityAEClass = daoFixture.getAsmUtils().getClassByFQName(TRANSFER_ENTITY_A_FQ_NAME).get();
        final EReference transferEntityAEReference = daoFixture.getAsmUtils().resolveReference(TRANSFER_RELATION_FQ_NAME).get();

        final UUID transferEntityBaseID = daoFixture.getDao()
                .create(transferEntityBaseEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", transferEntityBaseEClass.getName(), transferEntityBaseID);

        final UUID transferEntityAID = daoFixture.getDao()
                .createNavigationInstanceAt(transferEntityBaseID, transferEntityAEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", transferEntityAEClass.getName(), transferEntityAID);

        final Payload navigationResult = daoFixture.getDao()
                .getNavigationResultAt(transferEntityBaseID, transferEntityAEReference).get(0);
        final UUID relationContentID = navigationResult.getAs(idProviderClass, idProviderName);

        assertThat(relationContentID, equalTo(transferEntityAID));
    }

}
