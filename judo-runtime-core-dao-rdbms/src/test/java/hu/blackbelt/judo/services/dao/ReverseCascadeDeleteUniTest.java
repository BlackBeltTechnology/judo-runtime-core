package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO.QueryCustomizer;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
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
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class ReverseCascadeDeleteUniTest {

    private static final String MODEL_NAME = "M";
    private static final String DTO = MODEL_NAME + "._default_transferobjecttypes.";

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
    public void testOptionalAssociation(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Model model = getModel(0);

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized());

        final EClass aEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference reference = daoFixture.getAsmUtils().resolveReference(DTO + "A#b").get();

        final UUID aID = daoFixture.getDao()
                .create(aEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(idProviderClass, idProviderName);
        log.debug("A entity created with id {}", aID);

        final UUID bID = daoFixture.getDao()
                .createNavigationInstanceAt(aID, reference, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(idProviderClass, idProviderName);
        log.debug("B entity created with id {}", bID);

        assertTrue(daoFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(daoFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        daoFixture.getDao().delete(bEClass, bID);

        assertFalse(daoFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(daoFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
    }

    @Test
    @DisplayName("Test A X--> [1..1] B")
    public void testRequiredAssociation(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final Model model = getModel(1);

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized());

        final EClass aEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "B").get();

        final UUID bID = daoFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(idProviderClass, idProviderName);
        log.debug("B created with id {}", bID);

        final UUID aID = daoFixture.getDao()
                .create(aEClass, Payload.map("b", Payload.map(idProviderName, bID)),
                        QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(idProviderClass, idProviderName);
        log.debug("A created with id {}", aID);

        assertTrue(daoFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(daoFixture.getDao().getByIdentifier(bEClass, bID).isPresent());

        daoFixture.getDao().delete(bEClass, bID);

        assertFalse(daoFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(daoFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
    }

    @Test
    @DisplayName("Test A --> [1..1] B X--> [0..1] C")
    public void testChainAssociation(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
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

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized());

        final EClass aEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EClass bEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EClass cEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "C").get();
        final EReference referenceC = daoFixture.getAsmUtils().resolveReference(DTO + "B#c").get();

        final UUID bID = daoFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(idProviderClass, idProviderName);
        log.debug("B created with id {}", bID);

        final UUID aID = daoFixture.getDao()
                .create(aEClass, Payload.map("b", Payload.map(idProviderName, bID)),
                        QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(idProviderClass, idProviderName);
        log.debug("A created with id {}", aID);

        final UUID cID = daoFixture.getDao()
                .createNavigationInstanceAt(bID, referenceC, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(idProviderClass, idProviderName);
        log.debug("C created with id {}", cID);

        assertTrue(daoFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(daoFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
        assertTrue(daoFixture.getDao().getByIdentifier(cEClass, cID).isPresent());

        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().delete(cEClass, cID));

        assertTrue(daoFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertTrue(daoFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
        assertTrue(daoFixture.getDao().getByIdentifier(cEClass, cID).isPresent());
    }

    @Test
    public void testCircularDependencies(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
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

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized());

        final EClass aEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "A").get();
        final EReference bOfAReference = daoFixture.getAsmUtils().resolveReference(DTO + "A#b").get();
        final EClass bEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "B").get();
        final EReference cOfBReference = daoFixture.getAsmUtils().resolveReference(DTO + "B#c").get();
        final EClass cEClass = daoFixture.getAsmUtils().getClassByFQName(DTO + "C").get();
        final EReference aOfCReference = daoFixture.getAsmUtils().resolveReference(DTO + "C#a").get();

        final UUID aID = daoFixture.getDao()
                .create(aEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(idProviderClass, idProviderName);
        log.debug("A created with id {}", aID);

        final UUID bID = daoFixture.getDao()
                .create(bEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(idProviderClass, idProviderName);
        log.debug("B created with id {}", bID);

        final UUID cID = daoFixture.getDao()
                .create(cEClass, Payload.empty(), QueryCustomizer.<UUID>builder().mask(emptyMap()).build())
                .getAs(idProviderClass, idProviderName);
        log.debug("C created with id {}", cID);

        daoFixture.getDao().setReference(bOfAReference, aID, Collections.singleton(bID));
        daoFixture.getDao().setReference(cOfBReference, bID, Collections.singleton(cID));
        daoFixture.getDao().setReference(aOfCReference, cID, Collections.singleton(aID));

        daoFixture.getDao().delete(bEClass, bID);

        assertFalse(daoFixture.getDao().getByIdentifier(aEClass, aID).isPresent());
        assertFalse(daoFixture.getDao().getByIdentifier(bEClass, bID).isPresent());
        assertFalse(daoFixture.getDao().getByIdentifier(cEClass, cID).isPresent());
    }
}
