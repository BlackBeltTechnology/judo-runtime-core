package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.DAO.QueryCustomizer;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
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
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@ExtendWith(MockitoExtension.class)
@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class ReverseCascadeDeleteUniTest {

    private static final String MODEL_NAME = "M";
    private static final String DTO = MODEL_NAME + "._default_transferobjecttypes.";

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    private Model getModel(int associationLowerCardinality) {
        final EntityType aEntity = newEntityTypeBuilder().withName("A").build();
        aEntity.setMapping(newMappingBuilder().withTarget(aEntity).build());
        final EntityType bEntity = newEntityTypeBuilder().withName("B").build();
        bEntity.setMapping(newMappingBuilder().withTarget(bEntity).build());

        aEntity.getRelations().add(
                newOneWayRelationMemberBuilder()
                        .withName("b")
                        .withTarget(bEntity)
                        .withLower(associationLowerCardinality)
                        .withUpper(1)
                        .withCreateable(true)
                        .withReverseCascadeDelete(true)
                        .withMemberType(STORED)
                        .withRelationKind(ASSOCIATION)
                        .build());

        return newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(aEntity, bEntity)
                .build();
    }

    @Test
    @DisplayName("Test A X--> [0..1] B")
    public void testOptionalAssociation(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Model model = getModel(0);

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference reference = runtimeFixture.getAsmUtils().resolveReference(DTO + "A#b").get();

        final UUID aID = runtimeFixture.getDao()
                .create(aEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A entity created with id {}", aID);

        final UUID bID = runtimeFixture.getDao()
                .createNavigationInstanceAt(aID, reference, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B entity created with id {}", bID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        runtimeFixture.getDao().delete(bEClass, bID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
    }

    @Test
    @DisplayName("Test A X--> [1..1] B")
    public void testRequiredAssociation(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final Model model = getModel(1);

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized());

        final EClass aEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = runtimeFixture.getAsmUtils().getClassByFQName(DTO + "B").get();

        final UUID bID = runtimeFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("B created with id {}", bID);

        final UUID aID = runtimeFixture.getDao()
                .create(aEClass, Payload.map("b", Payload.map(runtimeFixture.getIdProvider().getName(), bID)),
                        QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("A created with id {}", aID);

        assertTrue(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        runtimeFixture.getDao().delete(bEClass, bID);

        assertFalse(runtimeFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(runtimeFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
    }

    @Test
    @DisplayName("Test A --> [1..1] B X--> [0..1] C")
    public void testChainAssociation(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        final EntityType aEntity = newEntityTypeBuilder().withName("A").build();
        aEntity.setMapping(newMappingBuilder().withTarget(aEntity).build());
        final EntityType bEntity = newEntityTypeBuilder().withName("B").build();
        bEntity.setMapping(newMappingBuilder().withTarget(bEntity).build());
        final EntityType cEntity = newEntityTypeBuilder().withName("C").build();
        cEntity.setMapping(newMappingBuilder().withTarget(cEntity).build());

        aEntity.getRelations().add(
                newOneWayRelationMemberBuilder()
                        .withName("b")
                        .withTarget(bEntity)
                        .withLower(1)
                        .withUpper(1)
                        .withMemberType(STORED)
                        .withRelationKind(ASSOCIATION)
                        .build());
        bEntity.getRelations().add(
                newOneWayRelationMemberBuilder()
                        .withName("c")
                        .withTarget(cEntity)
                        .withLower(0)
                        .withUpper(1)
                        .withMemberType(STORED)
                        .withRelationKind(ASSOCIATION)
                        .withReverseCascadeDelete(true)
                        .build());

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

        aEntity.getRelations().add(
                newOneWayRelationMemberBuilder()
                        .withName("b")
                        .withTarget(bEntity)
                        .withLower(0)
                        .withUpper(1)
                        .withCreateable(true)
                        .withReverseCascadeDelete(true)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build());
        bEntity.getRelations().add(
                newOneWayRelationMemberBuilder()
                        .withName("c")
                        .withTarget(cEntity)
                        .withLower(0)
                        .withUpper(1)
                        .withCreateable(true)
                        .withReverseCascadeDelete(true)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build());
        cEntity.getRelations().add(
                newOneWayRelationMemberBuilder()
                        .withName("a")
                        .withTarget(aEntity)
                        .withLower(0)
                        .withUpper(1)
                        .withCreateable(true)
                        .withReverseCascadeDelete(true)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build());

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
