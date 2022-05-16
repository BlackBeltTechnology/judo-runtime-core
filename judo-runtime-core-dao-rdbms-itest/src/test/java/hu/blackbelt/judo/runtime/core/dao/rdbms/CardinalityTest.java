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
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import liquibase.pro.packaged.r;
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
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
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

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
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

    private void checkRelationContentOf(JudoRuntimeFixture runtimeFixture, UUID ownerID, EReference eReference, Collection<UUID> expectedIDs) {
        final Collection<UUID> actualIDs = runtimeFixture.getDao()
                .getNavigationResultAt(ownerID, eReference).stream()
                .map(p -> p.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName()))
                .collect(Collectors.toSet());

        assertThat(actualIDs, equalTo(expectedIDs));
    }

    @Test
    public void testCardinalityViolationsSingleOptional(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModelWithCardinality(0, 1), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "Dao is not initialized");

        testCardinalityViolationsSingle(runtimeFixture);
    }

    @Test
    public void testCardinalityViolationsSingleRequired(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModelWithCardinality(1, 1), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "Dao is not initialized");

        testCardinalityViolationsSingle(runtimeFixture);
    }

    private void testCardinalityViolationsSingle(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.beginTransaction();
        final EClass ownerEClass = runtimeFixture.getAsmUtils().getClassByFQName(RELATION_OWNER_FQNAME).get();
        final EClass targetEClass = runtimeFixture.getAsmUtils().getClassByFQName(RELATION_TARGET_FQNAME).get();
        final EReference testEReference = runtimeFixture.getAsmUtils().resolveReference(RELATION_FQNAME).get();

        final UUID targetAID = runtimeFixture.getDao()
                .create(targetEClass, Payload.map(ATTRIBUTE_NAME, "A"),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", targetEClass.getName(), targetAID);

        final UUID ownerID = runtimeFixture.getDao()
                .create(ownerEClass, Payload.map(ATTRIBUTE_NAME, "Owner",
                                                 RELATION_NAME, Payload.map(runtimeFixture.getIdProvider().getName(), targetAID)),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", ownerEClass.getName(), ownerID);

        checkRelationContentOf(runtimeFixture, ownerID, testEReference, ImmutableSet.of(targetAID));
        runtimeFixture.commitTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao()
                .createNavigationInstanceAt(ownerID, testEReference, Payload.map(ATTRIBUTE_NAME, "B"),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build()));
        runtimeFixture.rollbackTransaction();

        if (testEReference.getLowerBound() == 1) {
            runtimeFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().setReference(testEReference, ownerID, ImmutableSet.of()));
            runtimeFixture.rollbackTransaction();

            runtimeFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().unsetReference(testEReference, ownerID));
            runtimeFixture.rollbackTransaction();
        }

        runtimeFixture.beginTransaction();
        checkRelationContentOf(runtimeFixture, ownerID, testEReference, ImmutableSet.of(targetAID));
        runtimeFixture.commitTransaction();
    }

    @Test
    public void testCardinalityViolationsInfiniteCollectionRequired(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModelWithCardinality(1, -1), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "Dao is not initialized");

        runtimeFixture.beginTransaction();
        final EClass ownerEClass = runtimeFixture.getAsmUtils().getClassByFQName(RELATION_OWNER_FQNAME).get();
        final EClass targetEClass = runtimeFixture.getAsmUtils().getClassByFQName(RELATION_TARGET_FQNAME).get();
        final EReference testEReference = runtimeFixture.getAsmUtils().resolveReference(RELATION_FQNAME).get();

        final UUID targetAID = runtimeFixture.getDao()
                .create(targetEClass, Payload.map(ATTRIBUTE_NAME, "A"),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{}created with id: {}", targetEClass.getName(), targetAID);

        final UUID ownerID = runtimeFixture.getDao()
                .create(ownerEClass, Payload.map(ATTRIBUTE_NAME, "Owner",
                                                 RELATION_NAME, ImmutableSet.of(Payload.map(runtimeFixture.getIdProvider().getName(), targetAID))),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{}created with id: {}", ownerEClass.getName(), ownerID);

        checkRelationContentOf(runtimeFixture, ownerID, testEReference, ImmutableSet.of(targetAID));
        runtimeFixture.commitTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().removeReferences(testEReference, ownerID, ImmutableSet.of(targetAID)));
        runtimeFixture.rollbackTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().setReference(testEReference, ownerID, ImmutableSet.of()));
        runtimeFixture.rollbackTransaction();

        runtimeFixture.beginTransaction();
        checkRelationContentOf(runtimeFixture, ownerID, testEReference, ImmutableSet.of(targetAID));
        runtimeFixture.commitTransaction();
    }

    @Test
    public void testCardinalityViolationsFiniteCollectionRequired(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModelWithCardinality(0, 2), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "Dao is not initialized");

        testCardinalityViolationsFiniteCollection(runtimeFixture);
    }

    @Test
    public void testCardinalityViolationsFiniteCollectionOptional(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getEsmModelWithCardinality(2, 2), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "Dao is not initialized");

        testCardinalityViolationsFiniteCollection(runtimeFixture);
    }

    private void testCardinalityViolationsFiniteCollection(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.beginTransaction();

        final EClass ownerEClass = runtimeFixture.getAsmUtils().getClassByFQName(RELATION_OWNER_FQNAME).get();
        final EClass targetEClass = runtimeFixture.getAsmUtils().getClassByFQName(RELATION_TARGET_FQNAME).get();
        final EReference testEReference = runtimeFixture.getAsmUtils().resolveReference(RELATION_FQNAME).get();

        final UUID targetAID = runtimeFixture.getDao()
                .create(targetEClass, Payload.map(ATTRIBUTE_NAME, "A"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", targetEClass.getName(), targetAID);

        final UUID targetBID = runtimeFixture.getDao()
                .create(targetEClass, Payload.map(ATTRIBUTE_NAME, "B"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", targetEClass.getName(), targetBID);

        final UUID targetCID = runtimeFixture.getDao()
                .create(targetEClass, Payload.map(ATTRIBUTE_NAME, "C"), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", targetEClass.getName(), targetCID);

        final UUID ownerID = runtimeFixture.getDao()
                .create(ownerEClass, Payload.map(ATTRIBUTE_NAME, "Owner",
                                                 RELATION_NAME, ImmutableSet.of(Payload.map(runtimeFixture.getIdProvider().getName(), targetAID),
                                                                                Payload.map(runtimeFixture.getIdProvider().getName(), targetBID))),
                        DAO.QueryCustomizer.<UUID>builder()
                                .mask(Collections.emptyMap())
                                .build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("{} created with id: {}", ownerEClass.getName(), ownerID);

        checkRelationContentOf(runtimeFixture, ownerID, testEReference, ImmutableSet.of(targetAID, targetBID));
        runtimeFixture.commitTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().createNavigationInstanceAt(ownerID, testEReference, Payload.map(ATTRIBUTE_NAME, "D"),
                DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build()));
        runtimeFixture.rollbackTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().setReference(testEReference, ownerID, ImmutableSet.of(targetAID, targetBID, targetCID)));
        runtimeFixture.rollbackTransaction();

        runtimeFixture.beginTransaction();
        assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().addReferences(testEReference, ownerID, ImmutableSet.of(targetCID)));
        runtimeFixture.rollbackTransaction();

        if (testEReference.getLowerBound() == testEReference.getUpperBound()) {
            runtimeFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().setReference(testEReference, ownerID, ImmutableSet.of(targetAID)));
            runtimeFixture.rollbackTransaction();

            runtimeFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().setReference(testEReference, ownerID, ImmutableSet.of()));
            runtimeFixture.rollbackTransaction();

            runtimeFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().removeReferences(testEReference, ownerID, ImmutableSet.of(targetAID)));
            runtimeFixture.rollbackTransaction();

            runtimeFixture.beginTransaction();
            assertThrows(IllegalArgumentException.class, () -> runtimeFixture.getDao().removeReferences(testEReference, ownerID, ImmutableSet.of(targetAID, targetBID)));
            runtimeFixture.rollbackTransaction();
        }

        runtimeFixture.beginTransaction();
        checkRelationContentOf(runtimeFixture, ownerID, testEReference, ImmutableSet.of(targetAID, targetBID));
        runtimeFixture.commitTransaction();
    }

}
