package hu.blackbelt.judo.services.dao;

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.measure.DurationType;
import hu.blackbelt.judo.meta.esm.measure.Measure;
import hu.blackbelt.judo.meta.esm.measure.Unit;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.*;
import hu.blackbelt.judo.meta.psm.accesspoint.ActorType;
import hu.blackbelt.judo.meta.psm.data.Attribute;
import hu.blackbelt.judo.meta.psm.data.util.builder.DataBuilders;
import hu.blackbelt.judo.meta.psm.derived.ExpressionDialect;
import hu.blackbelt.judo.meta.psm.derived.StaticNavigation;
import hu.blackbelt.judo.meta.psm.derived.util.builder.DerivedBuilders;
import hu.blackbelt.judo.meta.psm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.measure.util.builder.MeasureBuilders.newDurationUnitBuilder;
import static hu.blackbelt.judo.meta.esm.measure.util.builder.MeasureBuilders.newMeasureBuilder;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newPackageBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static hu.blackbelt.judo.meta.psm.accesspoint.util.builder.AccesspointBuilders.newActorTypeBuilder;
import static hu.blackbelt.judo.meta.psm.service.util.builder.ServiceBuilders.*;
import static hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders.newCardinalityBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class FilterTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    @AfterEach
    public void teardown(final RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    void testSimpleFilter(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final NumericType doubleType = newNumericTypeBuilder().withName("Double").withPrecision(15).withScale(4).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();
        final DateType dateType = newDateTypeBuilder().withName("Date").build();
        final TimestampType timestampType = newTimestampTypeBuilder().withName("Timestamp").withBaseUnit(DurationType.SECOND).build();
        final Unit day = newDurationUnitBuilder().withName("day").withRateDividend(1.0).withRateDivisor(1.0).withUnitType(DurationType.DAY).build();
        final Measure time = newMeasureBuilder().withName("Time").withUnits(Arrays.asList(day)).build();
        final NumericType yearType = newNumericTypeBuilder().withName("Day").withPrecision(9).withScale(0).build();

        final DataFeature firstNameOfPerson = newDataMemberBuilder()
                .withName("firstName")
                .withDataType(stringType)
                .withMemberType(MemberType.STORED)
                .build();
        final DataFeature lastNameOfPerson = newDataMemberBuilder()
                .withName("lastName")
                .withDataType(stringType)
                .withMemberType(MemberType.STORED)
                .build();
        final DataFeature nameOfPerson = newDataMemberBuilder()
                .withName("name")
                .withDataType(stringType)
                .withMemberType(MemberType.DERIVED)
                .withGetterExpression("self.firstName + ' ' + self.lastName")
                .build();
        final DataFeature birthDateOfPerson = newDataMemberBuilder()
                .withName("birthDate")
                .withDataType(dateType)
                .withMemberType(MemberType.STORED)
                .build();
        final DataFeature emailOfPerson = newDataMemberBuilder()
                .withName("email")
                .withRequired(true)
                .withIdentifier(true)
                .withDataType(stringType)
                .withMemberType(MemberType.STORED)
                .build();
        final EntityType personEntity = newEntityTypeBuilder()
                .withName("Person")
                .withAttributes(Arrays.asList(firstNameOfPerson, lastNameOfPerson, nameOfPerson, emailOfPerson, birthDateOfPerson))
                .build();
        final RelationFeature childrenOfPerson = newOneWayRelationMemberBuilder()
                .withName("children")
                .withLower(0).withUpper(-1)
                .withTarget(personEntity)
                .withCreateable(true).withUpdateable(true).withDeleteable(true)
                .withRelationKind(RelationKind.ASSOCIATION)
                .withMemberType(MemberType.STORED)
                .build();
        useEntityType(personEntity)
                .withMapping(newMappingBuilder().withTarget(personEntity).build())
                .withRelations(childrenOfPerson)
                .build();

        final TransferObjectType accessToken = newTransferObjectTypeBuilder()
                .withName("PersonToken")
                .withMapping(newMappingBuilder().withTarget(personEntity).build())
                .withAttributes(newDataMemberBuilder()
                        .withName("fullName")
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(nameOfPerson)
                        .withDataType(stringType)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("firstName")
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(firstNameOfPerson)
                        .withDataType(stringType)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("lastName")
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(lastNameOfPerson)
                        .withDataType(stringType)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("email")
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(emailOfPerson)
                        .withRequired(true)
                        .withDataType(stringType)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("birthDate")
                        .withMemberType(MemberType.MAPPED)
                        .withBinding(birthDateOfPerson)
                        .withDataType(dateType)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("ageAt2020")
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("`2021-01-01`!elapsedTimeFrom(self.birthDate) / 365.25")
                        .withDataType(yearType)
                        .build())
                .build();
        useTransferObjectType(accessToken)
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("children")
                        .withTarget(accessToken)
                        .withLower(0).withUpper(-1)
                        .withBinding(childrenOfPerson)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withCreateable(true).withUpdateable(true).withDeleteable(true)
                        .withMemberType(MemberType.MAPPED)
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(newPackageBuilder()
                        .withName("type")
                        .withElements(Arrays.asList(stringType, integerType, doubleType, booleanType, dateType, yearType, time))
                        .build())
                .withElements(newPackageBuilder()
                        .withName("entity")
                        .withElements(personEntity)
                        .build())
                .withElements(newPackageBuilder()
                        .withName("dto")
                        .withElements(accessToken)
                        .build())
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass personType = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (DTO_PACKAGE + ".entity.Person").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Default transfer object type of Person not found in ASM model"));

        final EClass accessTokenType = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (MODEL_NAME + ".dto.PersonToken").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("PersonToken type not found in ASM model"));
        final EReference childrenOfAccessTokenReference = accessTokenType.getEAllReferences().stream()
                .filter(r -> "children".equals(r.getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Children relation of Person not found in ASM model"));
        final EAttribute emailOfPersonTokenAttribute = accessTokenType.getEAllAttributes().stream().filter(a -> "email".equals(a.getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Email attribute not found"));
        final EAttribute firstNameOfPersonTokenAttribute = accessTokenType.getEAllAttributes().stream().filter(a -> "firstName".equals(a.getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Firstname attribute not found"));
        final EAttribute lastNameOfPersonTokenAttribute = accessTokenType.getEAllAttributes().stream().filter(a -> "lastName".equals(a.getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Lastname attribute not found"));
        ;

        final Payload p1 = daoFixture.getDao().create(personType, Payload.map(
                "firstName", "Jakab",
                "lastName", "Gipsz",
                "email", "gipsz.jakab@example.com",
                "birthDate", LocalDate.of(2001, 2, 3)
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID p1Id = p1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        log.debug("Created person #1 ({}): {}", p1Id, p1);

        final Payload p2 = daoFixture.getDao().create(personType, Payload.map(
                "firstName", "Elek",
                "lastName", "Teszt",
                "email", "teszt.elek@example.com",
                "birthDate", LocalDate.of(2001, 3, 1)
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID p2Id = p2.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        log.debug("Created person #2 ({}): {}", p2Id, p2);

        final Payload p3 = daoFixture.getDao().create(personType, Payload.map(
                "firstName", "Anyó",
                "lastName", "Nagy",
                "email", "nagyanyo@example.com",
                "birthDate", LocalDate.of(1972, 9, 10)
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID p3Id = p3.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        log.debug("Created person #3 ({}): {}", p3Id, p3);

        daoFixture.getDao().setReference(childrenOfAccessTokenReference, p3Id, Collections.singleton(p2Id));

        final List<Payload> peopleByEmail = daoFixture.getDao().search(accessTokenType, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.email == 'teszt.elek@example.com'")
                .build());
        log.debug("People by email: {}", peopleByEmail);
        assertThat(peopleByEmail, hasSize(1));
        assertThat(peopleByEmail.get(0).getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), equalTo(p2Id));

        final List<Payload> peopleByName = daoFixture.getDao().search(accessTokenType, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.fullName == 'Jakab Gipsz'")
                .build());
        log.debug("People by name: {}", peopleByName);
        assertThat(peopleByName, hasSize(1));
        assertThat(peopleByName.get(0).getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), equalTo(p1Id));

        final List<Payload> noChildrenByName = daoFixture.getDao().searchNavigationResultAt(p3Id, childrenOfAccessTokenReference, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.fullName == 'Teszt, Elek'")
                .build());
        log.debug("Children by name: {}", noChildrenByName);
        assertThat(noChildrenByName, hasSize(0));

        final List<Payload> childrenByName = daoFixture.getDao().searchNavigationResultAt(p3Id, childrenOfAccessTokenReference, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.fullName == 'Elek Teszt'")
                .build());
        log.debug("Children by name: {}", childrenByName);
        assertThat(childrenByName, hasSize(1));
        assertThat(childrenByName.get(0).getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), equalTo(p2Id));

        final List<Payload> peopleByBirthDate = daoFixture.getDao().search(accessTokenType, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.birthDate >= `2001-02-01` and this.birthDate < `2001-03-01`")
                .build());
        log.debug("People by birth date: {}", peopleByBirthDate);
        assertThat(peopleByBirthDate, hasSize(1));
        assertThat(peopleByBirthDate.get(0).getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName()), equalTo(p1Id));

        final List<Payload> peopleOver18a = daoFixture.getDao().search(accessTokenType, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.ageAt2020 >= 18.0")
                .orderByList(Arrays.asList(
                        DAO.OrderBy.builder().attribute(firstNameOfPersonTokenAttribute).descending(false).build(),
                        DAO.OrderBy.builder().attribute(lastNameOfPersonTokenAttribute).descending(false).build()
                ))
                .build());
        log.debug("People over 18 at 2020 (ordered): {}", peopleOver18a);
        assertThat(peopleOver18a.stream().map(p -> p.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList()), equalTo(Arrays.asList(p3Id, p2Id, p1Id)));
        final List<Payload> peopleOver18b = daoFixture.getDao().search(accessTokenType, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.ageAt2020 >= 18.0")
                .orderByList(Arrays.asList(
                        DAO.OrderBy.builder().attribute(firstNameOfPersonTokenAttribute).descending(true).build(),
                        DAO.OrderBy.builder().attribute(lastNameOfPersonTokenAttribute).descending(true).build()
                ))
                .build());
        log.debug("People over 18 at 2020 (reverse ordered): {}", peopleOver18b);
        assertThat(peopleOver18b.stream().map(p -> p.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName())).collect(Collectors.toList()), equalTo(Arrays.asList(p1Id, p2Id, p3Id)));

        final List<Payload> examplePeople = daoFixture.getDao().search(accessTokenType, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.email!matches('.*@example.com')")
                .build());
        log.debug("Example people: {}", examplePeople);
        assertThat(examplePeople, hasSize(3));

        final List<Payload> kPeople = daoFixture.getDao().search(accessTokenType, DAO.QueryCustomizer.<UUID>builder()
                .filter("this.fullName!matches('.*k.*')")
                .build());
        log.debug("People with name containing k: {}", kPeople);
        assertThat(kPeople, hasSize(2));
    }

    @Test
    public void testFilteredAccess(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final hu.blackbelt.judo.meta.psm.type.StringType stringType = TypeBuilders.newStringTypeBuilder().withName("String").withMaxLength(255).build();

        final Attribute colorOfBall = DataBuilders.newAttributeBuilder()
                .withName("color")
                .withRequired(true)
                .withDataType(stringType)
                .build();
        final hu.blackbelt.judo.meta.psm.data.EntityType ball = DataBuilders.newEntityTypeBuilder()
                .withName("Ball")
                .withAttributes(colorOfBall)
                .build();

        final hu.blackbelt.judo.meta.psm.service.TransferObjectType ballDTO = newMappedTransferObjectTypeBuilder()
                .withName("BallDTO")
                .withEntityType(ball)
                .withAttributes(newTransferAttributeBuilder()
                        .withName("color")
                        .withRequired(true)
                        .withDataType(stringType)
                        .withBinding(colorOfBall)
                        .build())
                .build();

        final StaticNavigation allRedBalls = DerivedBuilders.newStaticNavigationBuilder()
                .withName("allRedBalls")
                .withCardinality(newCardinalityBuilder().withLower(0).withUpper(-1).build())
                .withTarget(ball)
                .withGetterExpression(DerivedBuilders.newReferenceExpressionTypeBuilder()
                        .withDialect(ExpressionDialect.JQL)
                        .withExpression(MODEL_NAME + "::entity::Ball!filter(b | b.color == 'red')")
                        .build())
                .build();

        final ActorType actor = newActorTypeBuilder()
                .withName("Actor")
                .withRelations(newTransferObjectRelationBuilder()
                        .withName("redBalls")
                        .withAccess(true)
                        .withBinding(allRedBalls)
                        .withCardinality(newCardinalityBuilder().withLower(0).withUpper(-1).build())
                        .withTarget(ballDTO)
                        .build())
                .build();

        final hu.blackbelt.judo.meta.psm.namespace.Model model = NamespaceBuilders.newModelBuilder()
                .withName(getModelName())
                .withPackages(NamespaceBuilders.newPackageBuilder()
                        .withName("type")
                        .withElements(stringType)
                        .build())
                .withPackages(NamespaceBuilders.newPackageBuilder()
                        .withName("entity")
                        .withElements(ball, allRedBalls)
                        .build())
                .withPackages(NamespaceBuilders.newPackageBuilder()
                        .withName("dto")
                        .withElements(ballDTO)
                        .build())
                .withElements(actor)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass ballType = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (MODEL_NAME + ".dto.BallDTO").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Default transfer object type of Ball not found in ASM model"));

        final EClass actorType = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (MODEL_NAME + ".Actor").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Actor type not found in ASM model"));
        final EReference redBallsOfActor = actorType.getEAllReferences().stream()
                .filter(r -> "redBalls".equals(r.getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("RedBalls reference of actor not found in ASM model"));

        daoFixture.getDao().create(ballType, Payload.map("color", "red"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        daoFixture.getDao().create(ballType, Payload.map("color", "blue"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        daoFixture.getDao().create(ballType, Payload.map("color", "yellow"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        daoFixture.getDao().create(ballType, Payload.map("color", "green"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        daoFixture.getDao().create(ballType, Payload.map("color", "yellow"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        daoFixture.getDao().create(ballType, Payload.map("color", "red"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        daoFixture.getDao().create(ballType, Payload.map("color", "yellow"), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());

        final List<Payload> results = daoFixture.getDao().getAllReferencedInstancesOf(redBallsOfActor, ballType);
        log.debug("Results: {}", results);

        assertThat(results, hasSize(2));
    }

    @Test
    public void testFilteredAccessWithNavigationInside(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();

        final EnumerationType continent = newEnumerationTypeBuilder()
                .withName("Continent")
                .withMembers(newEnumerationMemberBuilder()
                        .withName("Africa")
                        .withOrdinal(0)
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withName("Asia")
                        .withOrdinal(1)
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withName("Europe")
                        .withOrdinal(2)
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withName("America")
                        .withOrdinal(3)
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withName("Antarctica")
                        .withOrdinal(4)
                        .build())
                .withMembers(newEnumerationMemberBuilder()
                        .withName("Australia")
                        .withOrdinal(5)
                        .build())
                .build();

        final EntityType country = newEntityTypeBuilder()
                .withName("Country")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("continent")
                        .withDataType(continent)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        country.setMapping(newMappingBuilder().withTarget(country).build());

        final EntityType city = newEntityTypeBuilder()
                .withName("City")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("country")
                        .withTarget(country)
                        .withLower(0).withUpper(1)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build())
                .build();
        city.setMapping(newMappingBuilder().withTarget(city).build());

        final EntityType person = newEntityTypeBuilder()
                .withName("Person")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("city")
                        .withTarget(city)
                        .withLower(0).withUpper(1)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build())
                .build();
        person.setMapping(newMappingBuilder().withTarget(person).build());

        final EntityType car = newEntityTypeBuilder()
                .withName("Car")
                .withAttributes(newDataMemberBuilder()
                        .withName("licensePlate")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("owner")
                        .withTarget(person)
                        .withLower(0).withUpper(1)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build())
                .build();
        car.setMapping(newMappingBuilder().withTarget(car).build());

        final TransferObjectType tester = newTransferObjectTypeBuilder()
                .withName("Tester")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("carsOfTesztElek")
                        .withLower(0).withUpper(-1)
                        .withTarget(car)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(MODEL_NAME + "::Car!filter(c | c.owner.name == 'Teszt Elek')")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("carsInBudapest")
                        .withLower(0).withUpper(-1)
                        .withTarget(car)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(MODEL_NAME + "::Car!filter(c | c.owner.city.name == 'Budapest')")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("carsInHungary")
                        .withLower(0).withUpper(-1)
                        .withTarget(car)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(MODEL_NAME + "::Car!filter(c | c.owner.city.country.name == 'Hungary')")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("carsOfKnownContinents")
                        .withLower(0).withUpper(-1)
                        .withTarget(car)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(MODEL_NAME + "::Car!filter(c | c.owner.city.country.continent!isDefined())")
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(stringType, continent, country, city, person, car, tester)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass countryType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".Country").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EClass cityType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".City").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EClass personType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".Person").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EClass carType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".Car").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EClass testerType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (MODEL_NAME + ".Tester").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EReference carsOfTesztElekReference = testerType.getEAllReferences().stream().filter(r -> "carsOfTesztElek".equals(r.getName())).findAny().get();
        final EReference carsInBudapestReference = testerType.getEAllReferences().stream().filter(r -> "carsInBudapest".equals(r.getName())).findAny().get();
        final EReference carsInHungaryReference = testerType.getEAllReferences().stream().filter(r -> "carsInHungary".equals(r.getName())).findAny().get();
        final EReference carsOfKnownContinentsReference = testerType.getEAllReferences().stream().filter(r -> "carsOfKnownContinents".equals(r.getName())).findAny().get();

        final Payload country1 = daoFixture.getDao().create(countryType, Payload.map("name", "Hungary", "continent", 2), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final Payload city1 = daoFixture.getDao().create(cityType, Payload.map("name", "Budapest", "country", country1), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final Payload city2 = daoFixture.getDao().create(cityType, Payload.map("name", "Debrecen", "country", country1), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final Payload p1 = daoFixture.getDao().create(personType, Payload.map("name", "Gipsz Jakab", "city", city1), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final Payload p2 = daoFixture.getDao().create(personType, Payload.map("name", "Teszt Elek", "city", city2), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final Payload c1 = daoFixture.getDao().create(carType, Payload.map("licensePlate", "ABC-123", "owner", p1), null);
        c1.remove("__$created");
        final Payload c2 = daoFixture.getDao().create(carType, Payload.map("licensePlate", "ABC-124", "owner", p2), null);
        c2.remove("__$created");

        final List<Payload> carsOfTesztElek = daoFixture.getDao().getAllReferencedInstancesOf(carsOfTesztElekReference, carsOfTesztElekReference.getEReferenceType());
        log.debug("Cars of Teszt Elek: {}", carsOfTesztElek);

        assertThat(carsOfTesztElek, hasSize(1));
        assertThat(carsOfTesztElek.get(0), equalTo(c2));

        final List<Payload> carsInBudapest = daoFixture.getDao().getAllReferencedInstancesOf(carsInBudapestReference, carsInBudapestReference.getEReferenceType());
        log.debug("Cars in Budapest: {}", carsInBudapest);

        assertThat(carsInBudapest, hasSize(1));
        assertThat(carsInBudapest.get(0), equalTo(c1));

        final Set<Payload> carsInHungary = new HashSet<>(daoFixture.getDao().getAllReferencedInstancesOf(carsInHungaryReference, carsInHungaryReference.getEReferenceType()));
        log.debug("Cars in Hungary: {}", carsInHungary);

        assertThat(carsInHungary, hasSize(2));
        assertThat(carsInHungary, equalTo(ImmutableSet.of(c1, c2)));

        final Set<Payload> carsOfKnownContinents = new HashSet<>(daoFixture.getDao().getAllReferencedInstancesOf(carsOfKnownContinentsReference, carsOfKnownContinentsReference.getEReferenceType()));
        log.debug("Cars of known continents: {}", carsOfKnownContinents);

        assertThat(carsOfKnownContinents, hasSize(2));
        assertThat(carsOfKnownContinents, equalTo(ImmutableSet.of(c1, c2)));
    }

    @Test
    public void testSingleFilter(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final EntityType system = newEntityTypeBuilder()
                .withName("System")
                .withAttributes(newDataMemberBuilder()
                        .withName("url")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("production")
                        .withDataType(booleanType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(system)
                .withMapping(newMappingBuilder().withTarget(system).build())
                .build();

        final TransferObjectType access = newTransferObjectTypeBuilder()
                .withName("Access")
                .withAttributes(newDataMemberBuilder()
                        .withName("productionUrl")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::System!any()!filter(s | s.production).url")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("developmentUrl")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::System!any()!filter(s | not s.production).url")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("systems")
                        .withTarget(system)
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::System")
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(stringType, booleanType, system, access)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass systemType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".System").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EClass accessType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (MODEL_NAME + ".Access").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EAttribute productionUrlOfAccessAttribute = accessType.getEAllAttributes().stream().filter(a -> "productionUrl".equals(a.getName())).findAny().get();
        final EAttribute developmentUrlOfAccessAttribute = accessType.getEAllAttributes().stream().filter(a -> "developmentUrl".equals(a.getName())).findAny().get();
        final EReference systemsOfAccessReference = accessType.getEAllReferences().stream().filter(r -> "systems".equals(r.getName())).findAny().get();

        final Payload system1 = daoFixture.getDao().create(systemType, Payload.map(
                "production", true,
                "url", "http://www.store.com"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID system1Id = system1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Payload productionUrlHolder = daoFixture.getDao().getStaticData(productionUrlOfAccessAttribute);
        assertThat(productionUrlHolder.getAs(String.class, productionUrlOfAccessAttribute.getName()), equalTo("http://www.store.com"));

        final Payload developmentUrlHolder = daoFixture.getDao().getStaticData(developmentUrlOfAccessAttribute);
        assertThat(developmentUrlHolder.getAs(String.class, developmentUrlOfAccessAttribute.getName()), nullValue());

        final Function<Payload, UUID> extractor = p -> p.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        final Set<UUID> allSystems1 = daoFixture.getDao().search(systemType, DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .stream().map(extractor).collect(Collectors.toSet());
        final Set<UUID> system1Only1 = daoFixture.getDao().search(systemType, DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .instanceIds(Collections.singleton(system1Id))
                        .build())
                .stream().map(extractor).collect(Collectors.toSet());
        final Set<UUID> noSystem1 = daoFixture.getDao().search(systemType, DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .instanceIds(Collections.singleton(UUID.randomUUID()))
                        .build())
                .stream().map(extractor).collect(Collectors.toSet());

        final Set<UUID> system1Only2 = daoFixture.getDao().searchReferencedInstancesOf(systemsOfAccessReference, systemsOfAccessReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build())
                .stream().map(extractor).collect(Collectors.toSet());
        final Set<UUID> noSystem2 = daoFixture.getDao().searchReferencedInstancesOf(systemsOfAccessReference, systemsOfAccessReference.getEReferenceType(), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .instanceIds(Collections.singleton(UUID.randomUUID()))
                        .build())
                .stream().map(extractor).collect(Collectors.toSet());

        assertThat(allSystems1, equalTo(Collections.singleton(system1Id)));
        assertThat(system1Only1, equalTo(Collections.singleton(system1Id)));
        assertThat(noSystem1, equalTo(Collections.emptySet()));
        assertThat(system1Only2, equalTo(Collections.singleton(system1Id)));
        assertThat(noSystem2, equalTo(Collections.emptySet()));
    }

    @Test
    public void testExists(final RdbmsDaoFixture daoFixture, final RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final EntityType ball = newEntityTypeBuilder()
                .withName("Ball")
                .withAttributes(newDataMemberBuilder()
                        .withName("color")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(ball)
                .withMapping(newMappingBuilder().withTarget(ball).build())
                .build();

        final EntityType box = newEntityTypeBuilder()
                .withName("Box")
                .withAttributes(newDataMemberBuilder()
                        .withName("hasNoBalls1")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.balls!empty()")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("hasBalls1")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("not self.balls!empty()")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("hasNoBalls2")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("not self.balls!exists(b | true)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("hasBalls2")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.balls!exists(b | true)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("hasNoBalls3")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.balls!count() == 0")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("balls")
                        .withTarget(ball)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build())
                .build();
        useEntityType(box)
                .withMapping(newMappingBuilder().withTarget(box).build())
                .build();

        final TransferObjectType access = newTransferObjectTypeBuilder()
                .withName("Access")
                .withAttributes(newDataMemberBuilder()
                        .withName("existsAnyBall")
                        .withDataType(booleanType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::Ball!exists(b | true)")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("noBall")
                        .withDataType(booleanType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::Ball!empty()")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("existsRedBall")
                        .withDataType(booleanType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::Ball!exists(b | b.color == 'RED')")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("noRedBall")
                        .withDataType(booleanType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression(MODEL_NAME + "::Ball!filter(b | b.color == 'RED')!empty()")
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(stringType, booleanType, box, ball, access)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass boxType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".Box").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EReference ballsOfBoxReference = boxType.getEAllReferences().stream().filter(r -> "balls".equals(r.getName())).findAny().get();
        final EClass ballType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (DTO_PACKAGE + ".Ball").equals(AsmUtils.getClassifierFQName(c))).findAny().get();
        final EClass accessType = daoFixture.getAsmUtils().all(EClass.class).filter(c -> (MODEL_NAME + ".Access").equals(AsmUtils.getClassifierFQName(c))).findAny().get();

        final Payload info1 = daoFixture.getDao().getStaticFeatures(accessType);
        log.debug("info #1: {}", info1);
        assertThat(info1, equalTo(ImmutableMap.of(
                "existsAnyBall", false,
                "existsRedBall", false,
                "noBall", true,
                "noRedBall", true
        )));

        final Payload ball1 = daoFixture.getDao().create(ballType, Payload.map(
                "color", "BLUE"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID ball1Id = ball1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Payload info2 = daoFixture.getDao().getStaticFeatures(accessType);
        log.debug("info #2: {}", info2);
        assertThat(info2, equalTo(ImmutableMap.of(
                "existsAnyBall", true,
                "existsRedBall", false,
                "noBall", false,
                "noRedBall", true
        )));

        final Payload ball2 = daoFixture.getDao().create(ballType, Payload.map(
                "color", "RED"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID ball2Id = ball2.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Payload info3 = daoFixture.getDao().getStaticFeatures(accessType);
        log.debug("info #3: {}", info3);
        assertThat(info3, equalTo(ImmutableMap.of(
                "existsAnyBall", true,
                "existsRedBall", true,
                "noBall", false,
                "noRedBall", false
        )));

        final Payload box1 = daoFixture.getDao().create(boxType, Payload.empty(), null);
        log.debug("Box1: {}", box1);
        final UUID box1Id = box1.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        assertThat(box1.get("hasBalls1"), equalTo(false));
        assertThat(box1.get("hasBalls2"), equalTo(false));
        assertThat(box1.get("hasNoBalls1"), equalTo(true));
        assertThat(box1.get("hasNoBalls2"), equalTo(true));
        assertThat(box1.get("hasNoBalls3"), equalTo(true));

        daoFixture.getDao().setReference(ballsOfBoxReference, box1Id, Collections.singleton(ball1Id));
        final Payload box1Reloaded = daoFixture.getDao().getByIdentifier(boxType, box1Id).get();
        log.debug("Box1 with balls: {}", box1Reloaded);
        assertThat(box1Reloaded.get("hasBalls1"), equalTo(true));
        assertThat(box1Reloaded.get("hasBalls2"), equalTo(true));
        assertThat(box1Reloaded.get("hasNoBalls1"), equalTo(false));
        assertThat(box1Reloaded.get("hasNoBalls2"), equalTo(false));
        assertThat(box1Reloaded.get("hasNoBalls3"), equalTo(false));

        final Payload box2 = daoFixture.getDao().create(boxType, Payload.empty(), null);
        log.debug("Box2: {}", box2);
        assertThat(box2.get("hasBalls1"), equalTo(false));
        assertThat(box2.get("hasBalls2"), equalTo(false));
        assertThat(box2.get("hasNoBalls1"), equalTo(true));
        assertThat(box2.get("hasNoBalls2"), equalTo(true));
        assertThat(box2.get("hasNoBalls3"), equalTo(true));
    }
}
