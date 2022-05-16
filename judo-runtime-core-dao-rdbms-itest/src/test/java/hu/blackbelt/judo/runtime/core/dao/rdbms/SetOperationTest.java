package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.ModelBuilder;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.TwoWayRelationMember;
import hu.blackbelt.judo.meta.esm.structure.util.builder.DataMemberBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.EntityTypeBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.MappingBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.TwoWayRelationMemberBuilder;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.meta.esm.type.util.builder.StringTypeBuilder;
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

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.ASSOCIATION;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@ExtendWith(MockitoExtension.class)
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class SetOperationTest {
    private static final String MODEL_NAME = "M";
    private static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private static final String GROUP_FQ_NAME = DTO_PACKAGE + ".Group";
    private static final String USER_FQ_NAME = DTO_PACKAGE + ".User";

    private static final String GROUP_RELATION_NAME = "group";
    private static final String USER_RELATION_NAME = "user";

    private static final String GROUP_RELATION_FQ_NAME = USER_FQ_NAME + "#" + GROUP_RELATION_NAME;
    private static final String USER_RELATION_FQ_NAME = GROUP_FQ_NAME + "#" + USER_RELATION_NAME;

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    private static Model getEsmModelWithTwoWayCardinality(int groupsLower, int groupsUpper, int usersLower, int usersUpper) {
        final StringType stringType = StringTypeBuilder.create().withName("string").withMaxLength(255).build();

        final EntityType groupEntity = EntityTypeBuilder.create()
                .withName("Group")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("name")
                                .withRequired(false)
                                .withMemberType(STORED)
                                .withDataType(stringType)
                                .build())
                .build();
        groupEntity.setMapping(MappingBuilder.create().withTarget(groupEntity).build());

        final EntityType userEntity = EntityTypeBuilder.create()
                .withName("User")
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName("name")
                                .withRequired(false)
                                .withMemberType(STORED)
                                .withDataType(stringType)
                                .build())
                .build();
        userEntity.setMapping(MappingBuilder.create().withTarget(userEntity).build());

        final TwoWayRelationMember userRelation = TwoWayRelationMemberBuilder.create()
                .withName(USER_RELATION_NAME)
                .withTarget(userEntity)
                .withLower(usersLower)
                .withUpper(usersUpper)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .withCreateable(true)
                .build();

        final TwoWayRelationMember groupRelation = TwoWayRelationMemberBuilder.create()
                .withName(GROUP_RELATION_NAME)
                .withTarget(groupEntity)
                .withLower(groupsLower)
                .withUpper(groupsUpper)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();

        userRelation.setPartner(groupRelation);
        groupRelation.setPartner(userRelation);

        userEntity.getRelations().add(groupRelation);
        groupEntity.getRelations().add(userRelation);

        return ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(asList(stringType, groupEntity, userEntity))
                .build();
    }

    private void checkEReferenceContentOf(JudoRuntimeFixture runtimeFixture,
                                          UUID groupID,
                                          EReference userEReference,
                                          Collection<UUID> expectedUserEReferenceIDs,
                                          EReference groupEReference) {
        Collection<UUID> actualUserEReferenceIDs = runtimeFixture.getDao()
                .getNavigationResultAt(groupID, userEReference)
                .stream()
                .map(p -> p.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                .collect(Collectors.toSet());

        assertThat(actualUserEReferenceIDs, equalTo(expectedUserEReferenceIDs));

        runtimeFixture.getDao().getAllOf(userEReference.getEReferenceType()).forEach(userPayload -> {
            final UUID userID = userPayload.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
            final Collection<UUID> actualGroupEReferenceIDs = runtimeFixture.getDao()
                    .getNavigationResultAt(userID, groupEReference).stream()
                    .map(p -> p.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                    .collect(Collectors.toSet());
            assertEquals(actualUserEReferenceIDs.contains(groupID), actualGroupEReferenceIDs.contains(userID),
                         "Group " + groupID + " <---> " + userID + " User reference is inconsistent");
        });
    }

    @Test
    public void testEntityToEntityTwoWayAssociationSingleToCollection(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModelWithTwoWayCardinality(0, 1, 0, -1), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        final EClass groupEClass = runtimeFixture.getAsmUtils().getClassByFQName(GROUP_FQ_NAME).get();
        final EClass userEClass = runtimeFixture.getAsmUtils().getClassByFQName(USER_FQ_NAME).get();
        final EReference userEReference = runtimeFixture.getAsmUtils().resolveReference(USER_RELATION_FQ_NAME).get();
        final EReference groupEReference = runtimeFixture.getAsmUtils().resolveReference(GROUP_RELATION_FQ_NAME).get();

        final UUID groupID = runtimeFixture.getDao()
                .create(groupEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", groupEClass.getName(), groupID);

        final UUID user1ID = runtimeFixture.getDao()
                .createNavigationInstanceAt(groupID, userEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", userEClass.getName(), user1ID);

        final UUID user2ID = runtimeFixture.getDao()
                .createNavigationInstanceAt(groupID, userEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", userEClass.getName(), user2ID);

        final UUID user3ID = runtimeFixture.getDao()
                .createNavigationInstanceAt(groupID, userEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", userEClass.getName(), user3ID);

        final UUID user4ID = runtimeFixture.getDao()
                .create(userEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", userEClass.getName(), user4ID);

        final UUID user5ID = runtimeFixture.getDao()
                .create(userEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", userEClass.getName(), user5ID);

        checkEReferenceContentOf(runtimeFixture, groupID, userEReference, ImmutableSet.of(user1ID, user2ID, user3ID), groupEReference);

        runtimeFixture.getDao().setReference(userEReference, groupID, ImmutableSet.of(user1ID));

        checkEReferenceContentOf(runtimeFixture, groupID, userEReference, ImmutableSet.of(user1ID), groupEReference);

        runtimeFixture.getDao().setReference(userEReference, groupID, ImmutableSet.of(user4ID, user5ID));

        checkEReferenceContentOf(runtimeFixture, groupID, userEReference, ImmutableSet.of(user4ID, user5ID), groupEReference);
    }

    @Test
    public void testEntityToEntityTwoWayAssociationRequiredToCollectionRequiredSingle(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModelWithTwoWayCardinality(1, 1, 0, -1), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        testEntityToEntityTwoWayAssociationSingleToCollection(runtimeFixture, false);
    }

    @Test
    public void testEntityToEntityTwoWayAssociationRequiredToCollectionOptionalSingle(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModelWithTwoWayCardinality(0, 1, 0, -1), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        testEntityToEntityTwoWayAssociationSingleToCollection(runtimeFixture, true);
    }

    private void testEntityToEntityTwoWayAssociationSingleToCollection(JudoRuntimeFixture runtimeFixture, boolean allowRemove) {
        runtimeFixture.beginTransaction();
        final EClass userEClass = runtimeFixture.getAsmUtils().getClassByFQName(USER_FQ_NAME).get();
        final EClass groupEClass = runtimeFixture.getAsmUtils().getClassByFQName(GROUP_FQ_NAME).get();
        final EReference groupEReference = runtimeFixture.getAsmUtils().resolveReference(GROUP_RELATION_FQ_NAME).get();
        final EReference userEReference = runtimeFixture.getAsmUtils().resolveReference(USER_RELATION_FQ_NAME).get();

        final UUID groupAID = runtimeFixture.getDao()
                .create(groupEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", groupEClass.getName(), groupAID);

        final UUID groupBID = runtimeFixture.getDao()
                .create(groupEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", groupEClass.getName(), groupBID);

        final UUID user1ID = runtimeFixture.getDao()
                .create(userEClass, Payload.map(GROUP_RELATION_NAME,
                                                Payload.map(runtimeFixture.getIdProvider().getName(), groupAID)), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", userEClass.getName(), user1ID);

        final UUID user2ID = runtimeFixture.getDao()
                .create(userEClass, Payload.map(GROUP_RELATION_NAME,
                                                Payload.map(runtimeFixture.getIdProvider().getName(), groupAID)), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", userEClass.getName(), user2ID);

        final UUID user3ID = runtimeFixture.getDao()
                .create(userEClass, Payload.map(GROUP_RELATION_NAME,
                                                Payload.map(runtimeFixture.getIdProvider().getName(), groupBID)), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with ID: {}", userEClass.getName(), user3ID);

        // do not change (admin) group members
        runtimeFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID, user2ID));

        checkEReferenceContentOf(runtimeFixture, groupAID, userEReference, ImmutableSet.of(user1ID, user2ID), groupEReference);
        checkEReferenceContentOf(runtimeFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);

        if (allowRemove) {
            runtimeFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID));
            checkEReferenceContentOf(runtimeFixture, groupAID, userEReference, ImmutableSet.of(user1ID), groupEReference);
            checkEReferenceContentOf(runtimeFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);

            runtimeFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID, user2ID));
            checkEReferenceContentOf(runtimeFixture, groupAID, userEReference, ImmutableSet.of(user1ID, user2ID), groupEReference);
            checkEReferenceContentOf(runtimeFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);

            runtimeFixture.getDao().removeReferences(userEReference, groupAID, ImmutableSet.of(user2ID));
            checkEReferenceContentOf(runtimeFixture, groupAID, userEReference, ImmutableSet.of(user1ID), groupEReference);
            checkEReferenceContentOf(runtimeFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);

            runtimeFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID, user2ID));
            checkEReferenceContentOf(runtimeFixture, groupAID, userEReference, ImmutableSet.of(user1ID, user2ID), groupEReference);
            checkEReferenceContentOf(runtimeFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);
        }

        runtimeFixture.commitTransaction();

        if (!allowRemove) {
            runtimeFixture.beginTransaction();
            assertThrows(IllegalStateException.class, () -> runtimeFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID)));
            runtimeFixture.rollbackTransaction();

            runtimeFixture.beginTransaction();
            assertThrows(IllegalStateException.class, () -> runtimeFixture.getDao().removeReferences(userEReference, groupAID, ImmutableSet.of(user2ID)));
            runtimeFixture.rollbackTransaction();
        }

        runtimeFixture.beginTransaction();
        assertThrows(IllegalStateException.class, () -> runtimeFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID, user2ID, user3ID)));
        runtimeFixture.rollbackTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalStateException.class, () -> runtimeFixture.getDao().addReferences(userEReference, groupAID, ImmutableSet.of(user3ID)));
        runtimeFixture.rollbackTransaction();

        runtimeFixture.beginTransaction();
        checkEReferenceContentOf(runtimeFixture, groupAID, userEReference, ImmutableSet.of(user1ID, user2ID), groupEReference);
        checkEReferenceContentOf(runtimeFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);

        runtimeFixture.getDao().setReference(groupEReference, user3ID, ImmutableSet.of(groupAID));

        checkEReferenceContentOf(runtimeFixture, groupAID, userEReference, ImmutableSet.of(user1ID, user2ID, user3ID), groupEReference);
        checkEReferenceContentOf(runtimeFixture, groupBID, userEReference, ImmutableSet.of(), groupEReference);
        runtimeFixture.commitTransaction();
    }

    @Test
    public void testConflictingCreateAndAttach(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Model model = getEsmModelWithTwoWayCardinality(0, -1, 0, -1);

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "Dao is not initialized");

        runtimeFixture.beginTransaction();

        final EClass userEClass = runtimeFixture.getAsmUtils().getClassByFQName(USER_FQ_NAME).get();
        final EClass groupEClass = runtimeFixture.getAsmUtils().getClassByFQName(GROUP_FQ_NAME).get();
        final EReference groupEReference = runtimeFixture.getAsmUtils().resolveReference(GROUP_RELATION_FQ_NAME).get();
        final EReference userEReference = runtimeFixture.getAsmUtils().resolveReference(USER_RELATION_FQ_NAME).get();

        final UUID groupAID = runtimeFixture.getDao()
                .create(groupEClass, Payload.map("name", "A"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        final UUID groupBID = runtimeFixture.getDao()
                .create(groupEClass, Payload.map("name", "B"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        final UUID groupCID = runtimeFixture.getDao()
                .create(groupEClass, Payload.map("name", "C"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());

        final UUID userAID = runtimeFixture.getDao()
                .createNavigationInstanceAt(groupAID, userEReference, Payload.map("name", "A"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", userEClass.getName(), userAID);

        checkEReferenceContentOf(runtimeFixture, groupAID, userEReference, ImmutableSet.of(userAID), groupEReference);
        checkEReferenceContentOf(runtimeFixture, groupBID, userEReference, ImmutableSet.of(), groupEReference);
        checkEReferenceContentOf(runtimeFixture, groupCID, userEReference, ImmutableSet.of(), groupEReference);

        final UUID userBID = runtimeFixture.getDao()
                .createNavigationInstanceAt(groupAID,
                                            userEReference,
                                            Payload.map("name", "B",
                                                        GROUP_RELATION_NAME, ImmutableSet.of(Payload.map(runtimeFixture.getIdProvider().getName(), groupAID),
                                                                                             Payload.map(runtimeFixture.getIdProvider().getName(), groupBID),
                                                                                             Payload.map(runtimeFixture.getIdProvider().getName(), groupCID))), DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", userEClass.getName(), userBID);

        checkEReferenceContentOf(runtimeFixture, groupAID, userEReference, ImmutableSet.of(userAID, userBID), groupEReference);
        checkEReferenceContentOf(runtimeFixture, groupBID, userEReference, ImmutableSet.of(userBID), groupEReference);
        checkEReferenceContentOf(runtimeFixture, groupCID, userEReference, ImmutableSet.of(userBID), groupEReference);

        runtimeFixture.commitTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao()
                .createNavigationInstanceAt(groupAID,
                                            userEReference,
                                            Payload.map("name", "C",
                                                        GROUP_RELATION_NAME, ImmutableSet.of(Payload.map(runtimeFixture.getIdProvider().getName(), groupBID),
                                                                                             Payload.map(runtimeFixture.getIdProvider().getName(), groupCID))), DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build()));
        runtimeFixture.rollbackTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao()
                .createNavigationInstanceAt(groupAID,
                                            userEReference,
                                            Payload.map("name", "D",
                                                        GROUP_RELATION_NAME, null), DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build()));
        runtimeFixture.rollbackTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao()
                .createNavigationInstanceAt(groupAID,
                                            userEReference,
                                            Payload.map("name", "E",
                                                        GROUP_RELATION_NAME, ImmutableSet.of()), DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build()));
        runtimeFixture.rollbackTransaction();

        runtimeFixture.beginTransaction();
        checkEReferenceContentOf(runtimeFixture, groupAID, userEReference, ImmutableSet.of(userAID, userBID), groupEReference);
        checkEReferenceContentOf(runtimeFixture, groupBID, userEReference, ImmutableSet.of(userBID), groupEReference);
        checkEReferenceContentOf(runtimeFixture, groupCID, userEReference, ImmutableSet.of(userBID), groupEReference);
        runtimeFixture.commitTransaction();
    }

}
