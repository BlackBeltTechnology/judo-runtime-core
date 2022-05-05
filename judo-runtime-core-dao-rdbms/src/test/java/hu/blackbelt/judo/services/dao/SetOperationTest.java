package hu.blackbelt.judo.services.dao;

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
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
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

    private void checkEReferenceContentOf(RdbmsDaoFixture daoFixture,
                                          UUID groupID,
                                          EReference userEReference,
                                          Collection<UUID> expectedUserEReferenceIDs,
                                          EReference groupEReference) {
        Collection<UUID> actualUserEReferenceIDs = daoFixture.getDao()
                .getNavigationResultAt(groupID, userEReference)
                .stream()
                .map(p -> p.getAs(idProviderClass, idProviderName))
                .collect(Collectors.toSet());

        assertThat(actualUserEReferenceIDs, equalTo(expectedUserEReferenceIDs));

        daoFixture.getDao().getAllOf(userEReference.getEReferenceType()).forEach(userPayload -> {
            final UUID userID = userPayload.getAs(idProviderClass, idProviderName);
            final Collection<UUID> actualGroupEReferenceIDs = daoFixture.getDao()
                    .getNavigationResultAt(userID, groupEReference).stream()
                    .map(p -> p.getAs(idProviderClass, idProviderName))
                    .collect(Collectors.toSet());
            assertEquals(actualUserEReferenceIDs.contains(groupID), actualGroupEReferenceIDs.contains(userID),
                         "Group " + groupID + " <---> " + userID + " User reference is inconsistent");
        });
    }

    @Test
    public void testEntityToEntityTwoWayAssociationSingleToCollection(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModelWithTwoWayCardinality(0, 1, 0, -1), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass groupEClass = daoFixture.getAsmUtils().getClassByFQName(GROUP_FQ_NAME).get();
        final EClass userEClass = daoFixture.getAsmUtils().getClassByFQName(USER_FQ_NAME).get();
        final EReference userEReference = daoFixture.getAsmUtils().resolveReference(USER_RELATION_FQ_NAME).get();
        final EReference groupEReference = daoFixture.getAsmUtils().resolveReference(GROUP_RELATION_FQ_NAME).get();

        final UUID groupID = daoFixture.getDao()
                .create(groupEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", groupEClass.getName(), groupID);

        final UUID user1ID = daoFixture.getDao()
                .createNavigationInstanceAt(groupID, userEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", userEClass.getName(), user1ID);

        final UUID user2ID = daoFixture.getDao()
                .createNavigationInstanceAt(groupID, userEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", userEClass.getName(), user2ID);

        final UUID user3ID = daoFixture.getDao()
                .createNavigationInstanceAt(groupID, userEReference, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", userEClass.getName(), user3ID);

        final UUID user4ID = daoFixture.getDao()
                .create(userEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", userEClass.getName(), user4ID);

        final UUID user5ID = daoFixture.getDao()
                .create(userEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", userEClass.getName(), user5ID);

        checkEReferenceContentOf(daoFixture, groupID, userEReference, ImmutableSet.of(user1ID, user2ID, user3ID), groupEReference);

        daoFixture.getDao().setReference(userEReference, groupID, ImmutableSet.of(user1ID));

        checkEReferenceContentOf(daoFixture, groupID, userEReference, ImmutableSet.of(user1ID), groupEReference);

        daoFixture.getDao().setReference(userEReference, groupID, ImmutableSet.of(user4ID, user5ID));

        checkEReferenceContentOf(daoFixture, groupID, userEReference, ImmutableSet.of(user4ID, user5ID), groupEReference);
    }

    @Test
    public void testEntityToEntityTwoWayAssociationRequiredToCollectionRequiredSingle(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModelWithTwoWayCardinality(1, 1, 0, -1), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        testEntityToEntityTwoWayAssociationSingleToCollection(daoFixture, false);
    }

    @Test
    public void testEntityToEntityTwoWayAssociationRequiredToCollectionOptionalSingle(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModelWithTwoWayCardinality(0, 1, 0, -1), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        testEntityToEntityTwoWayAssociationSingleToCollection(daoFixture, true);
    }

    private void testEntityToEntityTwoWayAssociationSingleToCollection(RdbmsDaoFixture daoFixture, boolean allowRemove) {
        daoFixture.beginTransaction();
        final EClass userEClass = daoFixture.getAsmUtils().getClassByFQName(USER_FQ_NAME).get();
        final EClass groupEClass = daoFixture.getAsmUtils().getClassByFQName(GROUP_FQ_NAME).get();
        final EReference groupEReference = daoFixture.getAsmUtils().resolveReference(GROUP_RELATION_FQ_NAME).get();
        final EReference userEReference = daoFixture.getAsmUtils().resolveReference(USER_RELATION_FQ_NAME).get();

        final UUID groupAID = daoFixture.getDao()
                .create(groupEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", groupEClass.getName(), groupAID);

        final UUID groupBID = daoFixture.getDao()
                .create(groupEClass, Payload.empty(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", groupEClass.getName(), groupBID);

        final UUID user1ID = daoFixture.getDao()
                .create(userEClass, Payload.map(GROUP_RELATION_NAME,
                                                Payload.map(idProviderName, groupAID)), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", userEClass.getName(), user1ID);

        final UUID user2ID = daoFixture.getDao()
                .create(userEClass, Payload.map(GROUP_RELATION_NAME,
                                                Payload.map(idProviderName, groupAID)), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", userEClass.getName(), user2ID);

        final UUID user3ID = daoFixture.getDao()
                .create(userEClass, Payload.map(GROUP_RELATION_NAME,
                                                Payload.map(idProviderName, groupBID)), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with ID: {}", userEClass.getName(), user3ID);

        // do not change (admin) group members
        daoFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID, user2ID));

        checkEReferenceContentOf(daoFixture, groupAID, userEReference, ImmutableSet.of(user1ID, user2ID), groupEReference);
        checkEReferenceContentOf(daoFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);

        if (allowRemove) {
            daoFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID));
            checkEReferenceContentOf(daoFixture, groupAID, userEReference, ImmutableSet.of(user1ID), groupEReference);
            checkEReferenceContentOf(daoFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);

            daoFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID, user2ID));
            checkEReferenceContentOf(daoFixture, groupAID, userEReference, ImmutableSet.of(user1ID, user2ID), groupEReference);
            checkEReferenceContentOf(daoFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);

            daoFixture.getDao().removeReferences(userEReference, groupAID, ImmutableSet.of(user2ID));
            checkEReferenceContentOf(daoFixture, groupAID, userEReference, ImmutableSet.of(user1ID), groupEReference);
            checkEReferenceContentOf(daoFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);

            daoFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID, user2ID));
            checkEReferenceContentOf(daoFixture, groupAID, userEReference, ImmutableSet.of(user1ID, user2ID), groupEReference);
            checkEReferenceContentOf(daoFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);
        }

        daoFixture.commitTransaction();

        if (!allowRemove) {
            daoFixture.beginTransaction();
            assertThrows(IllegalStateException.class, () -> daoFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID)));
            daoFixture.rollbackTransaction();

            daoFixture.beginTransaction();
            assertThrows(IllegalStateException.class, () -> daoFixture.getDao().removeReferences(userEReference, groupAID, ImmutableSet.of(user2ID)));
            daoFixture.rollbackTransaction();
        }

        daoFixture.beginTransaction();
        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().setReference(userEReference, groupAID, ImmutableSet.of(user1ID, user2ID, user3ID)));
        daoFixture.rollbackTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().addReferences(userEReference, groupAID, ImmutableSet.of(user3ID)));
        daoFixture.rollbackTransaction();

        daoFixture.beginTransaction();
        checkEReferenceContentOf(daoFixture, groupAID, userEReference, ImmutableSet.of(user1ID, user2ID), groupEReference);
        checkEReferenceContentOf(daoFixture, groupBID, userEReference, ImmutableSet.of(user3ID), groupEReference);

        daoFixture.getDao().setReference(groupEReference, user3ID, ImmutableSet.of(groupAID));

        checkEReferenceContentOf(daoFixture, groupAID, userEReference, ImmutableSet.of(user1ID, user2ID, user3ID), groupEReference);
        checkEReferenceContentOf(daoFixture, groupBID, userEReference, ImmutableSet.of(), groupEReference);
        daoFixture.commitTransaction();
    }

    @Test
    public void testConflictingCreateAndAttach(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Model model = getEsmModelWithTwoWayCardinality(0, -1, 0, -1);

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        daoFixture.beginTransaction();

        final EClass userEClass = daoFixture.getAsmUtils().getClassByFQName(USER_FQ_NAME).get();
        final EClass groupEClass = daoFixture.getAsmUtils().getClassByFQName(GROUP_FQ_NAME).get();
        final EReference groupEReference = daoFixture.getAsmUtils().resolveReference(GROUP_RELATION_FQ_NAME).get();
        final EReference userEReference = daoFixture.getAsmUtils().resolveReference(USER_RELATION_FQ_NAME).get();

        final UUID groupAID = daoFixture.getDao()
                .create(groupEClass, Payload.map("name", "A"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);

        final UUID groupBID = daoFixture.getDao()
                .create(groupEClass, Payload.map("name", "B"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);

        final UUID groupCID = daoFixture.getDao()
                .create(groupEClass, Payload.map("name", "C"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);

        final UUID userAID = daoFixture.getDao()
                .createNavigationInstanceAt(groupAID, userEReference, Payload.map("name", "A"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", userEClass.getName(), userAID);

        checkEReferenceContentOf(daoFixture, groupAID, userEReference, ImmutableSet.of(userAID), groupEReference);
        checkEReferenceContentOf(daoFixture, groupBID, userEReference, ImmutableSet.of(), groupEReference);
        checkEReferenceContentOf(daoFixture, groupCID, userEReference, ImmutableSet.of(), groupEReference);

        final UUID userBID = daoFixture.getDao()
                .createNavigationInstanceAt(groupAID,
                                            userEReference,
                                            Payload.map("name", "B",
                                                        GROUP_RELATION_NAME, ImmutableSet.of(Payload.map(idProviderName, groupAID),
                                                                                             Payload.map(idProviderName, groupBID),
                                                                                             Payload.map(idProviderName, groupCID))), DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", userEClass.getName(), userBID);

        checkEReferenceContentOf(daoFixture, groupAID, userEReference, ImmutableSet.of(userAID, userBID), groupEReference);
        checkEReferenceContentOf(daoFixture, groupBID, userEReference, ImmutableSet.of(userBID), groupEReference);
        checkEReferenceContentOf(daoFixture, groupCID, userEReference, ImmutableSet.of(userBID), groupEReference);

        daoFixture.commitTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao()
                .createNavigationInstanceAt(groupAID,
                                            userEReference,
                                            Payload.map("name", "C",
                                                        GROUP_RELATION_NAME, ImmutableSet.of(Payload.map(idProviderName, groupBID),
                                                                                             Payload.map(idProviderName, groupCID))), DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build()));
        daoFixture.rollbackTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao()
                .createNavigationInstanceAt(groupAID,
                                            userEReference,
                                            Payload.map("name", "D",
                                                        GROUP_RELATION_NAME, null), DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build()));
        daoFixture.rollbackTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao()
                .createNavigationInstanceAt(groupAID,
                                            userEReference,
                                            Payload.map("name", "E",
                                                        GROUP_RELATION_NAME, ImmutableSet.of()), DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build()));
        daoFixture.rollbackTransaction();

        daoFixture.beginTransaction();
        checkEReferenceContentOf(daoFixture, groupAID, userEReference, ImmutableSet.of(userAID, userBID), groupEReference);
        checkEReferenceContentOf(daoFixture, groupBID, userEReference, ImmutableSet.of(userBID), groupEReference);
        checkEReferenceContentOf(daoFixture, groupCID, userEReference, ImmutableSet.of(userBID), groupEReference);
        daoFixture.commitTransaction();
    }

}
