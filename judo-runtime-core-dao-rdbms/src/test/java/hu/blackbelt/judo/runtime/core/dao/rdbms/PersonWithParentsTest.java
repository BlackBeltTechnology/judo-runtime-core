package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.accesspoint.ActorType;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.*;
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

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.accesspoint.util.builder.AccesspointBuilders.newActorTypeBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class PersonWithParentsTest {

    public static final String MODEL_NAME = "F";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();
        final DateType dateType = newDateTypeBuilder().withName("Date").build();
        final EnumerationType sexType = newEnumerationTypeBuilder().withName("Sex")
                .withMembers(newEnumerationMemberBuilder()
                        .withOrdinal(0)
                        .withName("MALE")
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withOrdinal(1)
                        .withName("FEMALE")
                        .build())
                .build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(getModelName()).build();

        final EntityType person = newEntityTypeBuilder()
                .withName("Person")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("sex")
                        .withDataType(sexType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("birthDate")
                        .withDataType(dateType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("motherName")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.mother.name")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("fatherName")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.father.name")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("grandMother1Name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.mother.motherName")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("greatGrandMother1Name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.mother.mother.motherName")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("grandMother2Name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.father.motherName")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("grandFather1Name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.mother.fatherName")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("grandFather2Name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.father.fatherName")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("motherYoungerThanFather")
                        .withDataType(booleanType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.father.birthDate < self.mother.birthDate")
                        .build())
                .build();
        person.setMapping(newMappingBuilder().withTarget(person).build());

        final TwoWayRelationMember parents = newTwoWayRelationMemberBuilder()
                .withName("parents")
                .withTarget(person)
                .withLower(0).withUpper(2)
                .withRelationKind(RelationKind.ASSOCIATION)
                .withMemberType(MemberType.STORED)
                .build();

        final TwoWayRelationMember children = newTwoWayRelationMemberBuilder()
                .withName("children")
                .withTarget(person)
                .withLower(0).withUpper(-1)
                .withRelationKind(RelationKind.ASSOCIATION)
                .withMemberType(MemberType.STORED)
                .withPartner(parents)
                .build();
        useTwoWayRelationMember(parents)
                .withPartner(children)
                .build();

        useEntityType(person)
                .withRelations(children)
                .withRelations(parents)
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("mother")
                        .withTarget(person)
                        .withLower(0).withUpper(1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.parents!filter(p | p.sex == " + getModelName() + "::Sex#FEMALE)!any()")
                        //.withGetterExpression(getModelName() + "::Person!filter(m | self.parents!filter(p | p.sex == " + getModelName() + "::Sex#FEMALE)!any() == m)!any()")
                        //.withGetterExpression(getModelName() + "::Person!filter(m | self.parents!filter(p | p.sex == " + getModelName() + "::Sex#FEMALE)!contains(m))!any()")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("father")
                        .withTarget(person)
                        .withLower(0).withUpper(1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.parents!filter(p | p.sex == " + getModelName() + "::Sex#MALE)!any()")
                        //.withGetterExpression(getModelName() + "::Person!filter(f | self.parents!filter(p | p.sex == " + getModelName() + "::Sex#MALE)!any() == f)!any()")
                        //.withGetterExpression(getModelName() + "::Person!filter(f | self.parents!filter(p | p.sex == " + getModelName() + "::Sex#MALE)!contains(f))!any()")
                        .build())
                .build();

        TransferObjectType ap = newTransferObjectTypeBuilder()
                .withName("AP")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("women")
                        .withTarget(person)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Person!filter(p | p.sex == " + getModelName() + "::Sex#FEMALE)")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("mothers")
                        .withTarget(person)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Person!filter(m | " + getModelName() + "::Person!exists(p | p.parents!filter(pa | pa.sex == " + getModelName() + "::Sex#FEMALE)!contains(m)))")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("fathers")
                        .withTarget(person)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Person!filter(f | f.sex == " + getModelName() + "::Sex#MALE and f.children!exists(c | true))")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("peopleWithYoungerMotherThanFather")
                        .withTarget(person)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::Person!filter(p | p.motherYoungerThanFather)")
                        .build())
                .build();
        ActorType actor = newActorTypeBuilder().withName("actor").withPrincipal(ap).build();
        useTransferObjectType(ap).withActorType(actor).build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType, booleanType, dateType, sexType,
                person, ap, actor
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
    public void testWindowing(RdbmsDaoFixture daoFixture) {
        final EClass personType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Person").get();
        final EReference parents = personType.getEAllReferences().stream().filter(r -> "parents".equals(r.getName())).findAny().get();

        final Payload person1 = daoFixture.getDao().create(personType, map(
                "name", "Person1",
                "sex", 0,
                "birthDate", LocalDate.of(1987, 10, 11)
        ), null);
        log.debug("Saved person1: {}", person1);
        final UUID person1Id = person1.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload father1 = daoFixture.getDao().createNavigationInstanceAt(person1Id, parents, map(
                "name", "Father1",
                "sex", 0,
                "birthDate", LocalDate.of(1957, 1, 2)
        ), null);
        log.debug("Saved father1: {}", father1);
        final UUID father1Id = father1.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload mother1 = daoFixture.getDao().createNavigationInstanceAt(person1Id, parents, map(
                "name", "Mother1",
                "sex", 1,
                "birthDate", LocalDate.of(1960, 8, 17)
        ), null);
        log.debug("Saved mother1: {}", mother1);
        final UUID mother1Id = mother1.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload grandMother1 = daoFixture.getDao().createNavigationInstanceAt(mother1Id, parents, map(
                "name", "GrandMother",
                "sex", 1,
                "birthDate", LocalDate.of(1935, 3, 13)
        ), null);
        log.debug("Saved grandmother 1: {}", grandMother1);
        final UUID grandMother1Id = grandMother1.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload greatGrandMother1 = daoFixture.getDao().createNavigationInstanceAt(grandMother1Id, parents, map(
                "name", "GreatGrandMother",
                "sex", 1,
                "birthDate", LocalDate.of(1927, 9, 4)
        ), null);
        log.debug("Saved great-grandmother 1: {}", greatGrandMother1);
        final UUID greatGrandMother1Id = greatGrandMother1.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload person2 = daoFixture.getDao().create(personType, map(
                "name", "Person2",
                "sex", 1,
                "birthDate", LocalDate.of(1992, 4, 21)
        ), null);
        log.debug("Saved person2: {}", person2);
        final UUID person2Id = person2.getAs(UUID.class, daoFixture.getUuid().getName());

        final Payload person3 = daoFixture.getDao().create(personType, map(
                "name", "Person3",
                "sex", 0,
                "birthDate", LocalDate.of(2009, 8, 13)
        ), null);
        log.debug("Saved person3: {}", person3);
        final UUID person3Id = person3.getAs(UUID.class, daoFixture.getUuid().getName());

        log.debug("Set person2's parents");
        daoFixture.getDao().addReferences(parents, person2Id, Arrays.asList(mother1Id, father1Id));

        log.debug("Set person3's parents");
        daoFixture.getDao().addReferences(parents, person3Id, Arrays.asList(person1Id));

        log.debug("\n\n\n\n\n\n\n\n\n\nRunning queries...");

        final List<Payload> people = daoFixture.getDao().getByIdentifiers(personType, Arrays.asList(person1Id, person2Id, person3Id));
        log.debug("Person 1, 2 and 3: {}", people);
        assertThat(people, hasSize(3));
        final Optional<Payload> person1Loaded = people.stream().filter(p -> Objects.equals(p.getAs(UUID.class, daoFixture.getUuid().getName()), person1Id)).findAny();
        assertTrue(person1Loaded.isPresent());
        assertThat(person1Loaded.get().getAs(String.class, "motherName"), equalTo(mother1.getAs(String.class, "name")));
        assertThat(person1Loaded.get().getAs(String.class, "fatherName"), equalTo(father1.getAs(String.class, "name")));
        assertThat(person1Loaded.get().getAs(String.class, "grandMother1Name"), equalTo(grandMother1.getAs(String.class, "name")));
        assertThat(person1Loaded.get().getAs(String.class, "greatGrandMother1Name"), equalTo(greatGrandMother1.getAs(String.class, "name")));
        assertThat(person1Loaded.get().getAs(Boolean.class, "motherYoungerThanFather"), equalTo(Boolean.TRUE));
        assertThat(person1Loaded.get().getAsPayload("mother"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(mother1Id)));
        assertThat(person1Loaded.get().getAsPayload("father"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(father1Id)));
        assertThat(person1Loaded.get().getAsPayload("mother").getAsPayload("mother"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(grandMother1Id)));
        assertThat(person1Loaded.get().getAsPayload("mother").getAsPayload("mother").getAsPayload("mother"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(greatGrandMother1Id)));

        final Optional<Payload> person2Loaded = people.stream().filter(p -> Objects.equals(p.getAs(UUID.class, daoFixture.getUuid().getName()), person2Id)).findAny();
        assertTrue(person2Loaded.isPresent());
        assertThat(person2Loaded.get().getAs(String.class, "motherName"), equalTo(mother1.getAs(String.class, "name")));
        assertThat(person2Loaded.get().getAs(String.class, "fatherName"), equalTo(father1.getAs(String.class, "name")));
        assertThat(person2Loaded.get().getAs(String.class, "grandMother1Name"), equalTo(grandMother1.getAs(String.class, "name")));
        assertThat(person2Loaded.get().getAs(String.class, "greatGrandMother1Name"), equalTo(greatGrandMother1.getAs(String.class, "name")));
        assertThat(person2Loaded.get().getAsPayload("mother"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(mother1Id)));
        assertThat(person2Loaded.get().getAsPayload("father"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(father1Id)));
        assertThat(person2Loaded.get().getAsPayload("mother").getAsPayload("mother"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(grandMother1Id)));
        assertThat(person2Loaded.get().getAsPayload("mother").getAsPayload("mother").getAsPayload("mother"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(greatGrandMother1Id)));

        final Optional<Payload> person3Loaded = people.stream().filter(p -> Objects.equals(p.getAs(UUID.class, daoFixture.getUuid().getName()), person3Id)).findAny();
        assertTrue(person3Loaded.isPresent());
        assertThat(person3Loaded.get().getAs(String.class, "motherName"), nullValue());
        assertThat(person3Loaded.get().getAs(String.class, "fatherName"), equalTo(person1.getAs(String.class, "name")));
        assertThat(person3Loaded.get().getAs(String.class, "grandMother2Name"), equalTo(mother1.getAs(String.class, "name")));
        assertThat(person3Loaded.get().getAs(String.class, "grandFather2Name"), equalTo(father1.getAs(String.class, "name")));
        assertThat(person3Loaded.get().getAsPayload("mother"), nullValue());
        assertThat(person3Loaded.get().getAsPayload("father"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(person1Id)));
        assertThat(person3Loaded.get().getAsPayload("father").getAsPayload("mother"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(mother1Id)));
        assertThat(person3Loaded.get().getAsPayload("father").getAsPayload("mother").getAsPayload("mother"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(grandMother1Id)));
        assertThat(person3Loaded.get().getAsPayload("father").getAsPayload("mother").getAsPayload("mother").getAsPayload("mother"), hasEntry(equalTo(daoFixture.getUuid().getName()), equalTo(greatGrandMother1Id)));

        final EClass apType = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".AP").get();
        final EReference womenReference = apType.getEAllReferences().stream().filter(r -> "women".equals(r.getName())).findAny().get();
        final EReference mothersReference = apType.getEAllReferences().stream().filter(r -> "mothers".equals(r.getName())).findAny().get();
        final EReference fathersReference = apType.getEAllReferences().stream().filter(r -> "fathers".equals(r.getName())).findAny().get();
        final EReference peopleWithYoungerMotherThanFatherReference = apType.getEAllReferences().stream().filter(r -> "peopleWithYoungerMotherThanFather".equals(r.getName())).findAny().get();

        final Set<UUID> women = daoFixture.getDao().getAllReferencedInstancesOf(womenReference, personType).stream()
                .map(c -> c.getAs(UUID.class, daoFixture.getUuid().getName()))
                .collect(Collectors.toSet());
        log.debug("Women: {}", women);
        assertThat(women, equalTo(ImmutableSet.of(mother1Id, person2Id, grandMother1Id, greatGrandMother1Id)));

        final Set<UUID> mothers = daoFixture.getDao().getAllReferencedInstancesOf(mothersReference, personType).stream()
                .map(c -> c.getAs(UUID.class, daoFixture.getUuid().getName()))
                .collect(Collectors.toSet());
        log.debug("Mothers: {}", mothers);
        assertThat(mothers, equalTo(ImmutableSet.of(mother1Id, grandMother1Id, greatGrandMother1Id)));

        final Set<UUID> fathers = daoFixture.getDao().getAllReferencedInstancesOf(fathersReference, personType).stream()
                .map(c -> c.getAs(UUID.class, daoFixture.getUuid().getName()))
                .collect(Collectors.toSet());
        log.debug("Fathers: {}", fathers);
        assertThat(fathers, equalTo(ImmutableSet.of(father1Id, person1Id)));

        final Set<UUID> peopleWithYoungerMotherThanFather = daoFixture.getDao().getAllReferencedInstancesOf(peopleWithYoungerMotherThanFatherReference, personType).stream()
                .map(c -> c.getAs(UUID.class, daoFixture.getUuid().getName()))
                .collect(Collectors.toSet());
        log.debug("People with younger mother than father: {}", peopleWithYoungerMotherThanFather);
        assertThat(peopleWithYoungerMotherThanFather, equalTo(ImmutableSet.of(person1Id, person2Id)));
    }
}
