package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.data.AssociationEnd;
import hu.blackbelt.judo.meta.psm.data.Attribute;
import hu.blackbelt.judo.meta.psm.data.EntityType;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.service.MappedTransferObjectType;
import hu.blackbelt.judo.meta.psm.type.StringType;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.psm.data.util.builder.DataBuilders.*;
import static hu.blackbelt.judo.meta.psm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.psm.service.util.builder.ServiceBuilders.*;
import static hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders.newCardinalityBuilder;
import static hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class TwoWayRelationTest {

    public static final String MODEL_NAME = "twoway";
    public static final String A_ENTITY = "A";
    public static final String B_ENTITY = "B";
    public static final String A_DTO = "ADTO";
    public static final String B_DTO = "BDTO";

    public static final String BS_OF_A = "bsOfA";
    public static final String A_OF_B = "aOfB";

    public static final String NAME = "name";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getPsmModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(10).build();

        final Attribute nameOfA = newAttributeBuilder()
                .withName(NAME)
                .withDataType(stringType)
                .withRequired(true)
                .build();
        final Attribute nameOfB = newAttributeBuilder()
                .withName(NAME)
                .withDataType(stringType)
                .withRequired(true)
                .build();

        final EntityType a = newEntityTypeBuilder()
                .withName(A_ENTITY)
                .withAttributes(Arrays.asList(nameOfA))
                .build();
        final EntityType b = newEntityTypeBuilder()
                .withName(B_ENTITY)
                .withAttributes(Arrays.asList(nameOfB))
                .build();

        final AssociationEnd bsOfA = newAssociationEndBuilder()
                .withName(BS_OF_A)
                .withTarget(b)
                .withCardinality(newCardinalityBuilder().withLower(0).withUpper(-1).build())
                .build();

        final AssociationEnd aOfB = newAssociationEndBuilder()
                .withName(A_OF_B)
                .withTarget(a)
                .withCardinality(newCardinalityBuilder().withLower(0).withUpper(1).build())
                .withPartner(bsOfA)
                .build();

        useAssociationEnd(bsOfA).withPartner(aOfB).build();

        useEntityType(a).withRelations(bsOfA).build();
        useEntityType(b).withRelations(aOfB).build();

        final MappedTransferObjectType aDTO = newMappedTransferObjectTypeBuilder()
                .withName(A_DTO)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withRequired(true)
                        .withBinding(nameOfA)
                        .build())
                .withEntityType(a)
                .build();
        final MappedTransferObjectType bDTO = newMappedTransferObjectTypeBuilder()
                .withName(B_DTO)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withRequired(true)
                        .withBinding(nameOfB)
                        .build())
                .withEntityType(b)
                .build();

        useMappedTransferObjectType(aDTO)
                .withRelations(newTransferObjectRelationBuilder()
                        .withName(BS_OF_A)
                        .withCardinality(newCardinalityBuilder().withLower(0).withUpper(-1).build())
                        .withTarget(bDTO)
                        .withBinding(bsOfA)
                        .withEmbedded(true)
                        .build())
                .build();
        useMappedTransferObjectType(bDTO)
                .withRelations(newTransferObjectRelationBuilder()
                        .withName(A_OF_B)
                        .withCardinality(newCardinalityBuilder().withLower(0).withUpper(1).build())
                        .withTarget(aDTO)
                        .withBinding(aOfB)
                        .build())
                .build();

        Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(Arrays.asList(stringType, a, b, aDTO, bDTO)).build();
        return model;
    }

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getPsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    void testCount(RdbmsDaoFixture testFixture) {
        final Payload a1 = map(NAME, "a1");

        final Payload b1 = map(NAME, "b1");
        final Payload b2 = map(NAME, "b2");

        final EClass aDto = testFixture.getAsmUtils().getClassByFQName(MODEL_NAME + "." + A_DTO).get();
        final EClass bDto = testFixture.getAsmUtils().getClassByFQName(MODEL_NAME + "." + B_DTO).get();

        final EReference bsOfA = aDto.getEAllReferences().stream().filter(a -> BS_OF_A.equals(a.getName())).findAny().get();
        final EReference aOfB = bDto.getEAllReferences().stream().filter(a -> A_OF_B.equals(a.getName())).findAny().get();

        log.debug("Saving a1...");
        final Payload savedA1 = testFixture.getDao().create(aDto, a1, DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID a1Id = savedA1.getAs(testFixture.getIdProvider().getType(), testFixture.getIdProvider().getName());
        log.debug("  - saved a1: {}", a1Id);
        log.debug("Saving b1...");
        final Payload savedB1 = testFixture.getDao().create(bDto, b1, DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID b1Id = savedB1.getAs(testFixture.getIdProvider().getType(), testFixture.getIdProvider().getName());
        log.debug("  - saved b1: {}", b1Id);
        log.debug("Saving b2...");
        final Payload savedB2 = testFixture.getDao().create(bDto, b2, DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID b2Id = savedB2.getAs(testFixture.getIdProvider().getType(), testFixture.getIdProvider().getName());
        log.debug("  - saved b2: {}", b2Id);

        testFixture.getDao().addReferences(bsOfA, a1Id, Arrays.asList(b1Id, b2Id));

        final Optional<Payload> loadedA1 = testFixture.getDao().getByIdentifier(aDto, a1Id);
        assertTrue(loadedA1.isPresent());

        log.debug("Loaded a1: {}", loadedA1);

        testFixture.getDao().delete(bDto, b1Id);
        testFixture.getDao().delete(bDto, b2Id);
        testFixture.getDao().delete(aDto, a1Id);
    }
}
