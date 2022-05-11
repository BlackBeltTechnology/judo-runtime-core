package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.NamespaceElement;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.ModelBuilder;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.util.builder.DataMemberBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.EntityTypeBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.MappingBuilder;
import hu.blackbelt.judo.meta.esm.structure.util.builder.OneWayRelationMemberBuilder;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.meta.esm.type.util.builder.StringTypeBuilder;
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.ASSOCIATION;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class CardinalityTest {
    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private static final String RELATION_OWNER_NAME = "RelationOwner";
    private static final String RELATION_OWNER_FQNAME = DTO_PACKAGE + "." + RELATION_OWNER_NAME;

    private static final String RELATION_TARGET_NAME = "RelationTarget";
    private static final String RELATION_TARGET_FQNAME = DTO_PACKAGE + "." + RELATION_TARGET_NAME;

    private static final String ATTRIBUTE_NAME = "name";

    private static final String RELATION_NAME = "testRelation";
    private static final String RELATION_FQNAME = RELATION_OWNER_FQNAME + "#" + RELATION_NAME;

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

    private static Model getEsmModelWithCardinality(int lowerCardinality, int upperCardinality) {
        final StringType stringType = StringTypeBuilder.create().withName("string").withMaxLength(255).build();

        final Collection<NamespaceElement> namespaceElements = new HashSet<>(Collections.singleton(stringType));

        final EntityType relationTarget = EntityTypeBuilder.create()
                .withName(RELATION_TARGET_NAME)
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName(ATTRIBUTE_NAME)
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(stringType)
                                .build())
                .build();
        relationTarget.setMapping(MappingBuilder.create().withTarget(relationTarget).build());
        namespaceElements.add(relationTarget);

        final EntityType relationOwner = EntityTypeBuilder.create()
                .withName(RELATION_OWNER_NAME)
                .withAttributes(
                        DataMemberBuilder.create()
                                .withName(ATTRIBUTE_NAME)
                                .withRequired(true)
                                .withMemberType(STORED)
                                .withDataType(stringType)
                                .build())
                .withRelations(
                        OneWayRelationMemberBuilder.create()
                                .withName(RELATION_NAME)
                                .withTarget(relationTarget)
                                .withLower(lowerCardinality)
                                .withUpper(upperCardinality)
                                .withMemberType(STORED)
                                .withRelationKind(ASSOCIATION)
                                .build())
                .build();
        relationOwner.setMapping(MappingBuilder.create().withTarget(relationOwner).build());
        namespaceElements.add(relationOwner);

        return ModelBuilder.create()
                .withName(MODEL_NAME)
                .withElements(namespaceElements)
                .build();
    }

    private void checkRelationContentOf(RdbmsDaoFixture daoFixture, UUID ownerID, EReference eReference, Collection<UUID> expectedIDs) {
        final Collection<UUID> actualIDs = daoFixture.getDao()
                .getNavigationResultAt(ownerID, eReference).stream()
                .map(p -> p.getAs(idProviderClass, idProviderName))
                .collect(Collectors.toSet());

        assertThat(actualIDs, equalTo(expectedIDs));
    }

    @Test
    public void testCardinalityViolationsSingleOptional(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModelWithCardinality(0, 1), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        testCardinalityViolationsSingle(daoFixture);
    }

    @Test
    public void testCardinalityViolationsSingleRequired(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModelWithCardinality(1, 1), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        testCardinalityViolationsSingle(daoFixture);
    }

    private void testCardinalityViolationsSingle(RdbmsDaoFixture daoFixture) {
        daoFixture.beginTransaction();
        final EClass ownerEClass = daoFixture.getAsmUtils().getClassByFQName(RELATION_OWNER_FQNAME).get();
        final EClass targetEClass = daoFixture.getAsmUtils().getClassByFQName(RELATION_TARGET_FQNAME).get();
        final EReference testEReference = daoFixture.getAsmUtils().resolveReference(RELATION_FQNAME).get();

        final UUID targetAID = daoFixture.getDao()
                .create(targetEClass, Payload.map(ATTRIBUTE_NAME, "A"),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", targetEClass.getName(), targetAID);

        final UUID ownerID = daoFixture.getDao()
                .create(ownerEClass, Payload.map(ATTRIBUTE_NAME, "Owner",
                                                 RELATION_NAME, Payload.map(idProviderName, targetAID)),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", ownerEClass.getName(), ownerID);

        checkRelationContentOf(daoFixture, ownerID, testEReference, ImmutableSet.of(targetAID));
        daoFixture.commitTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao()
                .createNavigationInstanceAt(ownerID, testEReference, Payload.map(ATTRIBUTE_NAME, "B"),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build()));
        daoFixture.rollbackTransaction();

        if (testEReference.getLowerBound() == 1) {
            daoFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().setReference(testEReference, ownerID, ImmutableSet.of()));
            daoFixture.rollbackTransaction();

            daoFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().unsetReference(testEReference, ownerID));
            daoFixture.rollbackTransaction();
        }

        daoFixture.beginTransaction();
        checkRelationContentOf(daoFixture, ownerID, testEReference, ImmutableSet.of(targetAID));
        daoFixture.commitTransaction();
    }

    @Test
    public void testCardinalityViolationsInfiniteCollectionRequired(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModelWithCardinality(1, -1), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        daoFixture.beginTransaction();
        final EClass ownerEClass = daoFixture.getAsmUtils().getClassByFQName(RELATION_OWNER_FQNAME).get();
        final EClass targetEClass = daoFixture.getAsmUtils().getClassByFQName(RELATION_TARGET_FQNAME).get();
        final EReference testEReference = daoFixture.getAsmUtils().resolveReference(RELATION_FQNAME).get();

        final UUID targetAID = daoFixture.getDao()
                .create(targetEClass, Payload.map(ATTRIBUTE_NAME, "A"),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{}created with id: {}", targetEClass.getName(), targetAID);

        final UUID ownerID = daoFixture.getDao()
                .create(ownerEClass, Payload.map(ATTRIBUTE_NAME, "Owner",
                                                 RELATION_NAME, ImmutableSet.of(Payload.map(idProviderName, targetAID))),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{}created with id: {}", ownerEClass.getName(), ownerID);

        checkRelationContentOf(daoFixture, ownerID, testEReference, ImmutableSet.of(targetAID));
        daoFixture.commitTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().removeReferences(testEReference, ownerID, ImmutableSet.of(targetAID)));
        daoFixture.rollbackTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().setReference(testEReference, ownerID, ImmutableSet.of()));
        daoFixture.rollbackTransaction();

        daoFixture.beginTransaction();
        checkRelationContentOf(daoFixture, ownerID, testEReference, ImmutableSet.of(targetAID));
        daoFixture.commitTransaction();
    }

    @Test
    public void testCardinalityViolationsFiniteCollectionRequired(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModelWithCardinality(0, 2), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        testCardinalityViolationsFiniteCollection(daoFixture);
    }

    @Test
    public void testCardinalityViolationsFiniteCollectionOptional(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModelWithCardinality(2, 2), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "Dao is not initialized");

        testCardinalityViolationsFiniteCollection(daoFixture);
    }

    private void testCardinalityViolationsFiniteCollection(RdbmsDaoFixture daoFixture) {
        daoFixture.beginTransaction();

        final EClass ownerEClass = daoFixture.getAsmUtils().getClassByFQName(RELATION_OWNER_FQNAME).get();
        final EClass targetEClass = daoFixture.getAsmUtils().getClassByFQName(RELATION_TARGET_FQNAME).get();
        final EReference testEReference = daoFixture.getAsmUtils().resolveReference(RELATION_FQNAME).get();

        final UUID targetAID = daoFixture.getDao()
                .create(targetEClass, Payload.map(ATTRIBUTE_NAME, "A"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", targetEClass.getName(), targetAID);

        final UUID targetBID = daoFixture.getDao()
                .create(targetEClass, Payload.map(ATTRIBUTE_NAME, "B"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", targetEClass.getName(), targetBID);

        final UUID targetCID = daoFixture.getDao()
                .create(targetEClass, Payload.map(ATTRIBUTE_NAME, "C"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", targetEClass.getName(), targetCID);

        final UUID ownerID = daoFixture.getDao()
                .create(ownerEClass, Payload.map(ATTRIBUTE_NAME, "Owner",
                                                 RELATION_NAME, ImmutableSet.of(Payload.map(idProviderName, targetAID),
                                                                                Payload.map(idProviderName, targetBID))),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(idProviderClass, idProviderName);
        log.debug("{} created with id: {}", ownerEClass.getName(), ownerID);

        checkRelationContentOf(daoFixture, ownerID, testEReference, ImmutableSet.of(targetAID, targetBID));
        daoFixture.commitTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().createNavigationInstanceAt(ownerID, testEReference, Payload.map(ATTRIBUTE_NAME, "D"),
                DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build()));
        daoFixture.rollbackTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().setReference(testEReference, ownerID, ImmutableSet.of(targetAID, targetBID, targetCID)));
        daoFixture.rollbackTransaction();

        daoFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().addReferences(testEReference, ownerID, ImmutableSet.of(targetCID)));
        daoFixture.rollbackTransaction();

        if (testEReference.getLowerBound() == testEReference.getUpperBound()) {
            daoFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().setReference(testEReference, ownerID, ImmutableSet.of(targetAID)));
            daoFixture.rollbackTransaction();

            daoFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().setReference(testEReference, ownerID, ImmutableSet.of()));
            daoFixture.rollbackTransaction();

            daoFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().removeReferences(testEReference, ownerID, ImmutableSet.of(targetAID)));
            daoFixture.rollbackTransaction();

            daoFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().removeReferences(testEReference, ownerID, ImmutableSet.of(targetAID, targetBID)));
            daoFixture.rollbackTransaction();
        }

        daoFixture.beginTransaction();
        checkRelationContentOf(daoFixture, ownerID, testEReference, ImmutableSet.of(targetAID, targetBID));
        daoFixture.commitTransaction();
    }

}
