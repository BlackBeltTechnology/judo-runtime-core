package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
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

import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newNumericTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class PartialUpdateTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private static final String PERSON = "Person";
    private static final String NAME = "name";
    private static final String YEAR_OF_BIRTH = "yearOfBirth";
    private static final String FATHER = "father";
    private static final String PETS = "pets";

    private static final String PET = "Pet";

    private static final String ORIGINAL_NAME = "Gipsz Jakab";
    private static final String UPDATED_NAME = "Teszt Elek";
    private static final Integer ORIGINAL_YEAR_OF_BIRTH = 1980;
    private static final Integer UPDATED_YEAR_OF_BIRTH = 1981;

    private static final String FATHER1_NAME = "Father1";
    private static final String FATHER2_NAME = "Father2";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(getModelName()).build();

        final EntityType person = newEntityTypeBuilder()
                .withName(PERSON)
                .withAttributes(newDataMemberBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName(YEAR_OF_BIRTH)
                        .withDataType(integerType)
                        .withRequired(false)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        person.setMapping(newMappingBuilder().withTarget(person).build());

        final EntityType pet = newEntityTypeBuilder()
                .withName(PET)
                .withAttributes(newDataMemberBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        pet.setMapping(newMappingBuilder().withTarget(pet).build());

        useEntityType(person)
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName(FATHER)
                        .withLower(0).withUpper(1)
                        .withTarget(person)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName(PETS)
                        .withLower(0).withUpper(-1)
                        .withTarget(pet)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType, person, pet
        ));
        return model;
    }

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testPartialUpdate(RdbmsDaoFixture daoFixture) {
        final EClass personType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + "." + PERSON).get();
        final EClass petType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + "." + PET).get();
        final EReference petsReference = personType.getEAllReferences().stream().filter(r -> PETS.equals(r.getName())).findAny().get();

        final Payload father1 = daoFixture.getDao().create(personType, map(
                NAME, FATHER1_NAME
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved father1: {}", father1);
        final UUID father1Id = father1.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload father2 = daoFixture.getDao().create(personType, map(
                NAME, FATHER2_NAME
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved father2: {}", father2);
        final UUID father2Id = father2.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload cat = daoFixture.getDao().create(petType, map(
                NAME, "cat"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved cat: {}", cat);
        final UUID catId = cat.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload dog = daoFixture.getDao().create(petType, map(
                NAME, "dog"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved dog: {}", dog);
        final UUID dogId = dog.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload bunny = daoFixture.getDao().create(petType, map(
                NAME, "bunny"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        log.debug("Saved bunny: {}", bunny);
        final UUID bunnyId = bunny.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload original = daoFixture.getDao().create(personType, map(
                NAME, ORIGINAL_NAME,
                YEAR_OF_BIRTH, ORIGINAL_YEAR_OF_BIRTH,
                FATHER, map(
                        daoFixture.getIdProvider().getName(), father1Id
                ),
                PETS, Arrays.asList(
                        map(
                                daoFixture.getIdProvider().getName(), catId
                        )
                )
        ), null);
        final UUID personId = original.getAs(UUID.class, daoFixture.getUuid().getName());

        log.debug("Original person: {}", original);
        check(daoFixture, original, ORIGINAL_NAME, ORIGINAL_YEAR_OF_BIRTH, father1Id, Arrays.asList(catId));

        final Payload missingCollectionReference = daoFixture.getDao().update(personType, map(
                daoFixture.getIdProvider().getName(), personId,
                NAME, ORIGINAL_NAME,
                YEAR_OF_BIRTH, ORIGINAL_YEAR_OF_BIRTH,
                FATHER, map(
                        daoFixture.getIdProvider().getName(), father2Id
                )
        ), null);
        log.debug("Missing collection reference: {}", missingCollectionReference);
        check(daoFixture, missingCollectionReference, ORIGINAL_NAME, ORIGINAL_YEAR_OF_BIRTH, father2Id, Arrays.asList(catId));

        final Payload missingSingleReference = daoFixture.getDao().update(personType, map(
                daoFixture.getIdProvider().getName(), personId,
                NAME, ORIGINAL_NAME,
                YEAR_OF_BIRTH, ORIGINAL_YEAR_OF_BIRTH,
                PETS, Arrays.asList(
                        map(
                                daoFixture.getIdProvider().getName(), dogId
                        ),
                        map(
                                daoFixture.getIdProvider().getName(), catId
                        )
                )
        ), null);
        log.debug("Missing single reference: {}", missingSingleReference);
        check(daoFixture, missingSingleReference, ORIGINAL_NAME, ORIGINAL_YEAR_OF_BIRTH, father2Id, Arrays.asList(dogId, catId));

        daoFixture.getDao().setReference(petsReference, personId, Arrays.asList(dogId, bunnyId));

        final Payload missingOptionalAttribute = daoFixture.getDao().update(personType, map(
                daoFixture.getIdProvider().getName(), personId,
                NAME, UPDATED_NAME
        ), null);
        log.debug("Missing optional attribute: {}", missingOptionalAttribute);
        check(daoFixture, missingOptionalAttribute, UPDATED_NAME, ORIGINAL_YEAR_OF_BIRTH, father2Id, Arrays.asList(dogId, bunnyId));

        final Payload missingRequiredAttribute = daoFixture.getDao().update(personType, map(
                daoFixture.getIdProvider().getName(), personId,
                YEAR_OF_BIRTH, UPDATED_YEAR_OF_BIRTH
        ), null);
        log.debug("Missing required attribute: {}", missingRequiredAttribute);
        check(daoFixture, missingRequiredAttribute, UPDATED_NAME, UPDATED_YEAR_OF_BIRTH, father2Id, Arrays.asList(dogId, bunnyId));
    }

    private void check(final RdbmsDaoFixture daoFixture, final Payload person, final String expectedName, final Integer expectedYearOfBirth, final UUID expectedFatherId, final Collection<UUID> expectedPetIds) {
        assertThat(person, hasEntry(equalTo(NAME), equalTo(expectedName)));
        assertThat(person, hasEntry(equalTo(YEAR_OF_BIRTH), equalTo(expectedYearOfBirth)));
        if (expectedFatherId != null) {
            assertThat(person.getAsPayload(FATHER), hasEntry(equalTo(daoFixture.getIdProvider().getName()), equalTo(expectedFatherId)));
        } else {
            assertNull(person.getAsPayload(FATHER));
        }

        if (expectedPetIds != null) {
            expectedPetIds.forEach(expectedPetId -> assertThat(person.getAsCollectionPayload(PETS).stream()
                            .map(e -> (Map<String, Object>) e)
                            .collect(Collectors.toList()),
                    hasItem(hasEntry(equalTo(daoFixture.getIdProvider().getName()), equalTo(expectedPetId)))));
            assertTrue(person.getAsCollectionPayload(PETS).size() == expectedPetIds.size());
        }
    }
}
