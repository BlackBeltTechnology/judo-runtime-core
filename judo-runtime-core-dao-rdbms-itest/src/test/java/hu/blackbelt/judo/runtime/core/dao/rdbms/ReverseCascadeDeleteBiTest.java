package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.DAO.QueryCustomizer;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.TwoWayRelationMember;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.UUID;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.ASSOCIATION;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@ExtendWith(MockitoExtension.class)
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class ReverseCascadeDeleteBiTest {

    private static final String MODEL_NAME = "M";
    private static final String DTO = MODEL_NAME + "._default_transferobjecttypes.";

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    /**
     * <p>A [lower1..upper1] "a" <--> "b" [lower2..upper2] B</p>
     * <p/>
     * <p>This method returns a simple {@link Model} containing A and B {@link EntityType}s with
     * two way ({@link MemberType#STORED}) association (defined with parameters) between them.</p>
     *
     * @param lower1        "a" relation's lower cardinality
     * @param upper1        "a" relation's upper cardinality
     * @param revCasDelete1 "a" relation has reverse cascade delete property
     * @param lower2        "b" relation's lower cardinality
     * @param upper2        "b" relation's upper cardinality
     * @param revCasDelete2 "b" relation has reverse cascade delete property
     * @return {@link Model} containing A and B {@link EntityType}s with two way association between them.
     */
    private Model getModel(int lower1, int upper1, boolean revCasDelete1, int lower2, int upper2, boolean revCasDelete2) {
        final EntityType aEntity = newEntityTypeBuilder().withName("A").build();
        aEntity.setMapping(newMappingBuilder().withTarget(aEntity).build());
        final EntityType bEntity = newEntityTypeBuilder().withName("B").build();
        bEntity.setMapping(newMappingBuilder().withTarget(bEntity).build());

        final TwoWayRelationMember aOfB = newTwoWayRelationMemberBuilder()
                .withName("a" + (upper1 == -1 ? "s" : ""))
                .withTarget(aEntity)
                .withLower(lower1)
                .withUpper(upper1)
                .withCreateable(true)
                .withReverseCascadeDelete(revCasDelete1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();

        final TwoWayRelationMember bOfA = newTwoWayRelationMemberBuilder()
                .withName("b" + (upper2 == -1 ? "s" : ""))
                .withTarget(bEntity)
                .withLower(lower2)
                .withUpper(upper2)
                .withCreateable(true)
                .withReverseCascadeDelete(revCasDelete2)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();

        aOfB.setPartner(bOfA);
        bOfA.setPartner(aOfB);

        aEntity.getRelations().add(bOfA);
        bEntity.getRelations().add(aOfB);

        return newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(aEntity, bEntity)
                .build();
    }

    @Test
    @DisplayName("Test A [0..1] X<--> [0..1] B")
    public void testBothOptionalOneRevCasDelete(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Model model = getModel(0, 1, false, 0, 1, true);

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference referenceB = runtimeFixture.getAsmUtils().resolveReference(DTO + "A#b").get();

        final UUID aID = runtimeFixture.getDao()
                .create(aEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A entity created with id {}", aID);

        final UUID bID = runtimeFixture.getDao()
                .createNavigationInstanceAt(aID, referenceB, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B entity created with id {}", bID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        runtimeFixture.getDao().delete(bEClass, bID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
    }

    @Test
    @DisplayName("Test A [0..1] X<-->X [0..1] B")
    public void testBothOptionalBothRevCasDelete(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Model model = getModel(0, 1, true, 0, 1, true);

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference referenceB = runtimeFixture.getAsmUtils().resolveReference(DTO + "A#b").get();
        final EReference referenceA = runtimeFixture.getAsmUtils().resolveReference(DTO + "B#a").get();

        /////////////////////////////////////////
        // Delete from "b" relation

        UUID aID = runtimeFixture.getDao()
                .create(aEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A entity created with id {}", aID);

        UUID bID = runtimeFixture.getDao()
                .createNavigationInstanceAt(aID, referenceB, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B entity created with id {}", bID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        runtimeFixture.getDao().delete(bEClass, bID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        // Delete from "b" relation
        /////////////////////////////////////////
        // Delete from "a" relation

        bID = runtimeFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B entity created with id {}", bID);

        aID = runtimeFixture.getDao()
                .createNavigationInstanceAt(bID, referenceA, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A entity created with id {}", aID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        runtimeFixture.getDao().delete(aEClass, aID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        // Delete from "a" relation
        /////////////////////////////////////////
    }

    @Test
    @DisplayName("Test A [0..1] X<--> [1..1] B")
    public void testOptionalAndRequiredOneRevCasDelete1(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Model model = getModel(0, 1, false, 1, 1, true);

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference referenceA = runtimeFixture.getAsmUtils().resolveReference(DTO + "B#a").get();

        final UUID bID = runtimeFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", bID);

        final UUID aID = runtimeFixture.getDao()
                .createNavigationInstanceAt(bID, referenceA, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A created with id {}", aID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        runtimeFixture.getDao().delete(bEClass, bID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
    }

    @Test
    @DisplayName("Test A [0..1] <-->X [1..1] B")
    public void testOptionalAndRequiredOneRevCasDelete2(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Model model = getModel(0, 1, true, 1, 1, false);

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference referenceA = runtimeFixture.getAsmUtils().resolveReference(DTO + "B#a").get();

        final UUID bID = runtimeFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", bID);

        final UUID aID = runtimeFixture.getDao()
                .createNavigationInstanceAt(bID, referenceA, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A created with id {}", aID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        runtimeFixture.getDao().delete(aEClass, aID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
    }

    @Test
    @DisplayName("Test A [0..1] X<-->X [1..1] B")
    public void testOptionalAndRequiredBothRevCasDelete(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Model model = getModel(0, 1, true, 1, 1, true);

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference referenceA = runtimeFixture.getAsmUtils().resolveReference(DTO + "B#a").get();

        /////////////////////////////////////////
        // Delete from relation "a"

        UUID bID = runtimeFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", bID);

        UUID aID = runtimeFixture.getDao()
                .createNavigationInstanceAt(bID, referenceA, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A created with id {}", aID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        runtimeFixture.getDao().delete(aEClass, aID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        // Delete from relation "a"
        /////////////////////////////////////////
        // Delete from relation "b"

        bID = runtimeFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", bID);

        aID = runtimeFixture.getDao()
                .createNavigationInstanceAt(bID, referenceA, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A created with id {}", aID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        runtimeFixture.getDao().delete(bEClass, bID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        // Delete from relation "b"
        /////////////////////////////////////////
    }

    @Test
    @DisplayName("Test A [0..1] <-->X [0..*] B")
    public void testOptionalAndMultiRevCasDelete(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Model model = getModel(0, 1, true, 0, -1, false);

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference referenceB = runtimeFixture.getAsmUtils().resolveReference(DTO + "A#bs").get();

        final UUID aID = runtimeFixture.getDao()
                .create(aEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A created with id {}", aID);

        final UUID b1ID = runtimeFixture.getDao()
                .createNavigationInstanceAt(aID, referenceB, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", b1ID);

        final UUID b2ID = runtimeFixture.getDao()
                .createNavigationInstanceAt(aID, referenceB, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", b2ID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, b1ID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, b2ID).isPresent());

        runtimeFixture.getDao().delete(aEClass, aID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, b1ID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, b2ID).isPresent());
    }

    @Test
    @DisplayName("Test A [1..1] <-->X [0..*] B")
    public void testRequiredAndMultiRevCasDelete(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Model model = getModel(1, 1, true, 0, -1, false);

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference referenceB = runtimeFixture.getAsmUtils().resolveReference(DTO + "A#bs").get();

        final UUID aID = runtimeFixture.getDao()
                .create(aEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A created with id {}", aID);

        final UUID b1ID = runtimeFixture.getDao()
                .createNavigationInstanceAt(aID, referenceB, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", b1ID);

        final UUID b2ID = runtimeFixture.getDao()
                .createNavigationInstanceAt(aID, referenceB, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", b2ID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, b1ID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, b2ID).isPresent());

        runtimeFixture.getDao().delete(aEClass, aID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, b1ID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, b2ID).isPresent());
    }

    @Test
    @DisplayName("Test A [0..1] <--> [1..1] B [0..1] X<--> [0..1] C")
    public void testChainAssociation(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final EntityType aEntity = newEntityTypeBuilder().withName("A").build();
        aEntity.setMapping(newMappingBuilder().withTarget(aEntity).build());
        final EntityType bEntity = newEntityTypeBuilder().withName("B").build();
        bEntity.setMapping(newMappingBuilder().withTarget(bEntity).build());
        final EntityType cEntity = newEntityTypeBuilder().withName("C").build();
        cEntity.setMapping(newMappingBuilder().withTarget(cEntity).build());

        final TwoWayRelationMember aOfB = newTwoWayRelationMemberBuilder()
                .withName("a")
                .withTarget(aEntity)
                .withLower(0)
                .withUpper(1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();
        final TwoWayRelationMember bOfA = newTwoWayRelationMemberBuilder()
                .withName("b")
                .withTarget(bEntity)
                .withLower(1)
                .withUpper(1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();
        final TwoWayRelationMember cOfB = newTwoWayRelationMemberBuilder()
                .withName("c")
                .withTarget(cEntity)
                .withLower(0)
                .withUpper(1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .withReverseCascadeDelete(true)
                .build();
        final TwoWayRelationMember bOfC = newTwoWayRelationMemberBuilder()
                .withName("b")
                .withTarget(bEntity)
                .withLower(0)
                .withUpper(1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();

        bOfA.setPartner(aOfB);
        aOfB.setPartner(bOfA);
        cOfB.setPartner(bOfC);
        bOfC.setPartner(cOfB);

        aEntity.getRelations().add(bOfA);
        bEntity.getRelations().addAll(asList(aOfB, cOfB));
        cEntity.getRelations().add(bOfC);

        final Model model = newModelBuilder()
                .withName("M")
                .withElements(aEntity, bEntity, cEntity)
                .build();

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EClass cEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "C").get();
        final EReference referenceC = runtimeFixture.getAsmUtils().resolveReference(DTO + "B#c").get();

        final UUID bID = runtimeFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", bID);

        final UUID aID = runtimeFixture.getDao()
                .create(aEClass, Payload.map("b", Payload.map(runtimeFixture.getIdProvider().getName(), bID)),
                        QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A created with id {}", aID);

        final UUID cID = runtimeFixture.getDao()
                .createNavigationInstanceAt(bID, referenceC, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("C created with id {}", cID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(cEClass, cID).isPresent());

        assertThrows(IllegalStateException.class, () -> runtimeFixture.getDao().delete(cEClass, cID));

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(cEClass, cID).isPresent());
    }

    @Test
    public void testCircularDependencies(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final EntityType aEntity = newEntityTypeBuilder().withName("A").build();
        aEntity.setMapping(newMappingBuilder().withTarget(aEntity).build());
        final EntityType bEntity = newEntityTypeBuilder().withName("B").build();
        bEntity.setMapping(newMappingBuilder().withTarget(bEntity).build());
        final EntityType cEntity = newEntityTypeBuilder().withName("C").build();
        cEntity.setMapping(newMappingBuilder().withTarget(cEntity).build());

        final TwoWayRelationMember bOfA = newTwoWayRelationMemberBuilder()
                .withName("b")
                .withTarget(bEntity)
                .withLower(0)
                .withUpper(1)
                .withCreateable(true)
                .withReverseCascadeDelete(true)
                .withMemberType(MemberType.STORED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();
        final TwoWayRelationMember cOfA = newTwoWayRelationMemberBuilder()
                .withName("c")
                .withTarget(cEntity)
                .withLower(0)
                .withUpper(1)
                .withCreateable(true)
                .withReverseCascadeDelete(true)
                .withMemberType(MemberType.STORED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();
        final TwoWayRelationMember aOfB = newTwoWayRelationMemberBuilder()
                .withName("a")
                .withTarget(aEntity)
                .withLower(0)
                .withUpper(1)
                .withCreateable(true)
                .withReverseCascadeDelete(true)
                .withMemberType(MemberType.STORED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();
        final TwoWayRelationMember cOfB = newTwoWayRelationMemberBuilder()
                .withName("c")
                .withTarget(cEntity)
                .withLower(0)
                .withUpper(1)
                .withCreateable(true)
                .withReverseCascadeDelete(true)
                .withMemberType(MemberType.STORED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();
        final TwoWayRelationMember aOfC = newTwoWayRelationMemberBuilder()
                .withName("a")
                .withTarget(aEntity)
                .withLower(0)
                .withUpper(1)
                .withCreateable(true)
                .withReverseCascadeDelete(true)
                .withMemberType(MemberType.STORED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();
        final TwoWayRelationMember bOfC = newTwoWayRelationMemberBuilder()
                .withName("b")
                .withTarget(bEntity)
                .withLower(0)
                .withUpper(1)
                .withCreateable(true)
                .withReverseCascadeDelete(true)
                .withMemberType(MemberType.STORED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .build();

        useTwoWayRelationMember(aOfB).withPartner(bOfA).build();
        useTwoWayRelationMember(cOfB).withPartner(bOfC).build();
        useTwoWayRelationMember(aOfC).withPartner(cOfA).build();
        useTwoWayRelationMember(bOfC).withPartner(cOfB).build();
        useTwoWayRelationMember(bOfA).withPartner(aOfB).build();
        useTwoWayRelationMember(cOfA).withPartner(aOfC).build();

        useEntityType(aEntity).withRelations(bOfA, cOfA).build();
        useEntityType(bEntity).withRelations(aOfB, cOfB).build();
        useEntityType(cEntity).withRelations(aOfC, bOfC).build();

        Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(aEntity, bEntity, cEntity)
                .build();

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EReference bOfAReference = runtimeFixture.getAsmUtils().resolveReference(DTO + "A#b").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference cOfBReference = runtimeFixture.getAsmUtils().resolveReference(DTO + "B#c").get();
        final EClass cEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "C").get();
        final EReference aOfCReference = runtimeFixture.getAsmUtils().resolveReference(DTO + "C#a").get();

        final UUID aID = runtimeFixture.getDao()
                .create(aEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A created with id {}", aID);

        final UUID bID = runtimeFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", bID);

        final UUID cID = runtimeFixture.getDao()
                .create(cEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("C created with id {}", cID);

        runtimeFixture.getDao().setReference(bOfAReference, aID, Collections.singleton(bID));
        runtimeFixture.getDao().setReference(cOfBReference, bID, Collections.singleton(cID));
        runtimeFixture.getDao().setReference(aOfCReference, cID, Collections.singleton(aID));

        runtimeFixture.getDao().delete(bEClass, bID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(cEClass, cID).isPresent());
    }

}
