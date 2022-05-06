package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.measure.DurationType;
import hu.blackbelt.judo.meta.esm.measure.DurationUnit;
import hu.blackbelt.judo.meta.esm.measure.Measure;
import hu.blackbelt.judo.meta.esm.measure.MeasuredType;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.*;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.esm.type.DateType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.measure.util.builder.MeasureBuilders.*;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class ParameterizedQueryTest {

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
    public void testParameterizedQuery(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.setIgnoreSdk(false);
        final Measure time = newMeasureBuilder().withName("Time").withSymbol("t").build();
        final DurationUnit day = newDurationUnitBuilder().withName("day").withSymbol("d").withRateDividend(86400).withRateDivisor(1).withUnitType(DurationType.DAY).build();
        useMeasure(time).withUnits(Arrays.asList(day)).build();

        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();
        final DateType dateType = newDateTypeBuilder().withName("Date").build();
        final MeasuredType dayDurationType = newMeasuredTypeBuilder().withName("Day").withPrecision(15).withScale(4).withStoreUnit(day).build();

        final TransferObjectType queryParameter = newTransferObjectTypeBuilder()
                .withName("QueryParameter")
                .withAttributes(newDataMemberBuilder()
                        .withName("age")
                        .withDataType(integerType)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("location")
                        .withDataType(stringType)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .build();

        final TransferObjectType queryParameter2 = newTransferObjectTypeBuilder()
                .withName("QueryParameter2")
                .withGeneralizations(newGeneralizationBuilder().withTarget(queryParameter).build())
                .build();

        final TransferObjectType durationQueryParameter = newTransferObjectTypeBuilder()
                .withName("IntervalQueryParameter")
                .withAttributes(newDataMemberBuilder()
                        .withName("duration")
                        .withDataType(dayDurationType)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .build();

        final DataMember overAge = newDataMemberBuilder()
                .withIsQuery(true)
                .withName("overage")
                .withDataType(booleanType)
                .withMemberType(MemberType.DERIVED)
                .withGetterExpression("self.age > input.age")
                .withInput(queryParameter2)
                .build();

        final EntityType exam = newEntityTypeBuilder()
                .withName("Exam")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("code")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("date")
                        .withDataType(dateType)
                        .build())
                .build();
        useEntityType(exam).withMapping(newMappingBuilder().withTarget(exam).build()).build();

        final EntityType person = newEntityTypeBuilder()
                .withName("Person")
                .withAttributes(newDataMemberBuilder()
                        .withName("name")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("city")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("age")
                        .withDataType(integerType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("exams")
                        .withTarget(exam)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.STORED)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .build())
                .withAttributes(overAge)
                .withQueries(overAge)
                .build();
        final OneWayRelationMember lastExams = newOneWayRelationMemberBuilder()
                .withName("lastExams")
                .withLower(0).withUpper(-1)
                .withTarget(exam)
                .withRelationKind(RelationKind.ASSOCIATION)
                .withMemberType(MemberType.DERIVED)
                .withGetterExpression("self.exams!filter(e | " + MODEL_NAME + "::Date!now()!elapsedTimeFrom(e.date) < input.duration)")
                .withInput(durationQueryParameter)
                .build();
        useEntityType(person)
                .withMapping(newMappingBuilder().withTarget(person).build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("mother")
                        .withLower(0).withUpper(1)
                        .withTarget(person)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("father")
                        .withLower(0).withUpper(1)
                        .withTarget(person)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(lastExams)
                .withQueries(lastExams)
                .build();

        final OneWayRelationMember people = newOneWayRelationMemberBuilder()
                .withName("people")
                .withIsQuery(true)
                .withLower(0).withUpper(-1)
                .withTarget(person)
                .withMemberType(MemberType.DERIVED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .withGetterExpression(MODEL_NAME + "::Person!filter(p | p.overage)")
                .withInput(queryParameter)
                .build();

        final OneWayRelationMember peopleWithOldParent = newOneWayRelationMemberBuilder()
                .withName("peopleWithOldParent")
                .withIsQuery(true)
                .withLower(0).withUpper(-1)
                .withTarget(person)
                .withMemberType(MemberType.DERIVED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .withGetterExpression(MODEL_NAME + "::Person!filter(p | p.mother.age > input.age or p.father.age > input.age)")
                .withInput(queryParameter)
                .build();

        final OneWayRelationMember peopleWithOldParentLivingAt = newOneWayRelationMemberBuilder()
                .withName("peopleWithOldParentLivingAt")
                .withIsQuery(true)
                .withLower(0).withUpper(-1)
                .withTarget(person)
                .withMemberType(MemberType.DERIVED)
                .withRelationKind(RelationKind.ASSOCIATION)
                .withGetterExpression(MODEL_NAME + "::Person!filter(p | p.mother.age > input.age and p.mother.city == input.location" +
                        " or p.father.age > input.age and p.father.city == input.location)")
                .withInput(queryParameter)
                .build();


        final TransferObjectType access = newTransferObjectTypeBuilder()
                .withName("Access")
                .withRelations(people, peopleWithOldParent, peopleWithOldParentLivingAt)
                .withQueries(people, peopleWithOldParent, peopleWithOldParentLivingAt)
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(stringType, integerType, booleanType, dateType, time, dayDurationType, person, exam, access, queryParameter, queryParameter2, durationQueryParameter)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass personType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Person").get();
        final EClass examType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Exam").get();
        final EClass accessType = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".Access").get();
        final EReference peopleReference = accessType.getEAllReferences().stream().filter(r -> "people".equals(r.getName())).findAny().get();
        final EReference lastExamsReference = personType.getEAllReferences().stream().filter(r -> "lastExams".equals(r.getName())).findAny().get();
        final EReference peopleWithOldParentReference = accessType.getEAllReferences().stream().filter(r -> "peopleWithOldParent".equals(r.getName())).findAny().get();
        final EReference peopleWithOldParentLivingAtReference = accessType.getEAllReferences().stream().filter(r -> "peopleWithOldParentLivingAt".equals(r.getName())).findAny().get();
        final EReference motherReference = personType.getEAllReferences().stream().filter(r -> "mother".equals(r.getName())).findAny().get();
        final EReference fatherReference = personType.getEAllReferences().stream().filter(r -> "father".equals(r.getName())).findAny().get();

        final Function<Payload, UUID> idExtractor = p -> p.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Payload exam1 = daoFixture.getDao().create(examType, Payload.map(
                "name", "Oracle Certified Professional, Java SE 11 Developer",
                "code", "1Z0-819",
                "date", LocalDate.now().minus(400, ChronoUnit.DAYS)
        ), null);

        final Payload exam2 = daoFixture.getDao().create(examType, Payload.map(
                "name", "Oracle Certified Master, Java EE 6 Enterprise Architect",
                "code", "1Z0-807",
                "date", LocalDate.now().minus(1, ChronoUnit.MONTHS)
        ), null);

        final Payload p1 = daoFixture.getDao().create(personType, Payload.map(
                "name", "Gipsz Jakab",
                "age", 20,
                "city", "Budapest",
                "exams", Arrays.asList(exam1, exam2)
        ), null);
        final UUID p1Id = idExtractor.apply(p1);
        assertThat(p1.get("overage"), nullValue());
        final Optional<Boolean> p1Overage = daoFixture.getDao().search(personType, DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.singletonMap("overage", true))
                        .instanceIds(Collections.singleton(p1Id))
                        .parameters(Collections.singletonMap("age", 21))
                        .build())
                .stream()
                .map(p -> p.getAs(Boolean.class, "overage"))
                .findAny();
        assertThat(p1Overage.get(), equalTo(Boolean.FALSE));
        final List<Payload> lastExamsOfP1 = daoFixture.getDao().searchNavigationResultAt(p1Id, lastExamsReference, DAO.QueryCustomizer.<UUID>builder()
                .parameters(Collections.singletonMap("duration", 365))
                .build());
        assertThat(lastExamsOfP1, equalTo(Collections.singletonList(exam2)));
        final Payload p2 = daoFixture.getDao().create(personType, Payload.map(
                "name", "Teszt Elek",
                "age", 16,
                "city", "Debrecen"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID p2Id = idExtractor.apply(p2);
        final Boolean p2Overage = daoFixture.getDao().search(personType, DAO.QueryCustomizer.<UUID>builder()
                        .instanceIds(Collections.singleton(p2Id))
                        .parameters(Collections.singletonMap("age", 21))
                        .build())
                .get(0).getAs(Boolean.class, "overage");
        assertThat(p2Overage, nullValue());

        final Payload p3 = daoFixture.getDao().create(personType, Payload.map(
                "name", "Nagy Piroska",
                "age", 50,
                "city", "Miskolc"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID p3Id = idExtractor.apply(p3);
        final Payload p4 = daoFixture.getDao().create(personType, Payload.map(
                "name", "Kiss Péter",
                "age", 56,
                "city", "Debrecen"
        ), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        final UUID p4Id = idExtractor.apply(p4);

        daoFixture.getDao().setReference(motherReference, p1Id, ImmutableSet.of(p3Id));
        daoFixture.getDao().setReference(fatherReference, p1Id, ImmutableSet.of(p4Id));
        daoFixture.getDao().setReference(motherReference, p2Id, ImmutableSet.of(p3Id));

        final List<Payload> over18 = daoFixture.getDao().searchReferencedInstancesOf(peopleReference, personType, DAO.QueryCustomizer.<UUID>builder()
                .parameters(ImmutableMap.of(
                        "age", 18
                )).build());
        final Set<UUID> over18Ids = over18.stream().map(idExtractor).collect(Collectors.toSet());
        assertThat(over18Ids, equalTo(ImmutableSet.of(p1Id, p3Id, p4Id)));

        final List<Payload> over12 = daoFixture.getDao().searchReferencedInstancesOf(peopleReference, personType, DAO.QueryCustomizer.<UUID>builder()
                .parameters(ImmutableMap.of(
                        "age", 12
                )).build());
        final Set<UUID> over12Ids = over12.stream().map(idExtractor).collect(Collectors.toSet());
        assertThat(over12Ids, equalTo(ImmutableSet.of(p1Id, p2Id, p3Id, p4Id)));

        final List<Payload> peopleWithOldParentRes = daoFixture.getDao().searchReferencedInstancesOf(peopleWithOldParentReference, personType, DAO.QueryCustomizer.<UUID>builder()
                .parameters(ImmutableMap.of(
                        "age", 55
                )).build());
        final Set<UUID> peopleWithOldParentIds = peopleWithOldParentRes.stream().map(idExtractor).collect(Collectors.toSet());
        assertThat(peopleWithOldParentIds, equalTo(ImmutableSet.of(p1Id)));

        final List<Payload> peopleWithOldParent2 = daoFixture.getDao().searchReferencedInstancesOf(peopleWithOldParentReference, personType, DAO.QueryCustomizer.<UUID>builder()
                .parameters(ImmutableMap.of(
                        "age", 40
                )).build());
        final Set<UUID> peopleWithOldParent2Ids = peopleWithOldParent2.stream().map(idExtractor).collect(Collectors.toSet());
        assertThat(peopleWithOldParent2Ids, equalTo(ImmutableSet.of(p1Id, p2Id)));

        final List<Payload> peopleWithOldParentLivingAtDebrecen = daoFixture.getDao().searchReferencedInstancesOf(peopleWithOldParentLivingAtReference, personType, DAO.QueryCustomizer.<UUID>builder()
                .parameters(ImmutableMap.of(
                        "age", 40,
                        "location", "Debrecen"
                )).build());
        final Set<UUID> peopleWithOldParentLivingAtDebrecenIds = peopleWithOldParentLivingAtDebrecen.stream().map(idExtractor).collect(Collectors.toSet());
        assertThat(peopleWithOldParentLivingAtDebrecenIds, equalTo(ImmutableSet.of(p1Id)));

        final List<Payload> peopleWithOldParentLivingAtMiskolc = daoFixture.getDao().searchReferencedInstancesOf(peopleWithOldParentLivingAtReference, personType, DAO.QueryCustomizer.<UUID>builder()
                .parameters(ImmutableMap.of(
                        "age", 55,
                        "location", "Miskolc"
                )).build());
        final Set<UUID> peopleWithOldParentLivingAtMiskolcIds = peopleWithOldParentLivingAtMiskolc.stream().map(idExtractor).collect(Collectors.toSet());
        assertThat(peopleWithOldParentLivingAtMiskolcIds, equalTo(Collections.emptySet()));
    }
}
