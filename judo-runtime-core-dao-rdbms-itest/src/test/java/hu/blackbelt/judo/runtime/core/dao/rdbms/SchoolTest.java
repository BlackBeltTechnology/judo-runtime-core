package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.accesspoint.ActorType;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.type.EnumerationType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.accesspoint.util.builder.AccesspointBuilders.newActorTypeBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class SchoolTest {

    public static final String MODEL_NAME = "schools";

    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {
        StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        NumericType doubleType = newNumericTypeBuilder().withName("Double").withPrecision(15).withScale(4).build();
        EnumerationType genderType = newEnumerationTypeBuilder().withName("Gender").withMembers(
                newEnumerationMemberBuilder().withName("MALE").withOrdinal(0).build(),
                newEnumerationMemberBuilder().withName("FEMALE").withOrdinal(1).build()
        ).build();

        Model model = NamespaceBuilders.newModelBuilder().withName(getModelName()).build();

        EntityType person = newEntityTypeBuilder().withName("Person")
                .withAttributes(
                        newDataMemberBuilder().withName("name").withDataType(stringType).withRequired(true).withMemberType(MemberType.STORED).build(),
                        newDataMemberBuilder().withName("height").withDataType(doubleType).withRequired(true).withMemberType(MemberType.STORED).build(),
                        newDataMemberBuilder().withName("gender").withDataType(genderType).withRequired(false).withMemberType(MemberType.STORED).build()
                ).build();

        person.setMapping(newMappingBuilder().withTarget(person).build());

        EntityType student = newEntityTypeBuilder().withName("Student")
                .withGeneralizations(newGeneralizationBuilder().withTarget(person))
                .withRelations(
                        newOneWayRelationMemberBuilder().withName("parents")
                                .withRelationKind(RelationKind.ASSOCIATION)
                                .withTarget(person)
                                .withLower(0).withUpper(-1)
                                .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder().withName("mother")
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withTarget(person)
                        .withLower(1).withUpper(1)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.parents!filter(p | p.gender == schools::Gender#FEMALE)!any()").build())
                .build();

        student.setMapping(newMappingBuilder().withTarget(student).build());

        EntityType clazz = newEntityTypeBuilder().withName("Class")
                .withAttributes(
                        newDataMemberBuilder().withName("name").withDataType(stringType).withRequired(true).withMemberType(MemberType.STORED).build())
                .withRelations(newOneWayRelationMemberBuilder().withName("students")
                            .withLower(0).withUpper(-1)
                            .withRelationKind(RelationKind.AGGREGATION)
                            .withTarget(student)
                            .withMemberType(MemberType.STORED)
                            .build())
                .withRelations(newOneWayRelationMemberBuilder().withName("tallestStudent")
                        .withLower(0).withUpper(1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withTarget(student)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.students!head(s | s.height DESC)")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder().withName("tallestStudents")
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withTarget(student)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.students!heads(s | s.height DESC)")
                        .build())
                .build();

        clazz.setMapping(newMappingBuilder().withTarget(clazz).build());

        EntityType school = newEntityTypeBuilder().withName("School")
                .withAttributes(
                        newDataMemberBuilder().withName("name").withDataType(stringType).withRequired(true).withMemberType(MemberType.STORED).build())
                .withRelations(newOneWayRelationMemberBuilder().withName("classes")
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withTarget(clazz)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder().withName("tallestStudentMothers")
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withTarget(person)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.classes.tallestStudent.mother").build())
                .withRelations(newOneWayRelationMemberBuilder().withName("tallestStudentsMothers")
                        .withLower(0).withUpper(-1)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .withTarget(person)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.classes.tallestStudents.mother").build())
                .build();
        school.setMapping(newMappingBuilder().withTarget(school).build());

        TransferObjectType ap = newTransferObjectTypeBuilder()
                .withName("AP")
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("tallestStudentInEachClass")
                        .withTarget(student)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::School.classes.tallestStudent")
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("tallestStudentInEachClassMother")
                        .withTarget(student)
                        .withLower(0).withUpper(-1)
                        .withMemberType(MemberType.DERIVED)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withGetterExpression(getModelName() + "::School.classes.tallestStudent.mother")
                        .build())
                .withAttributes(newDataMemberBuilder()
                    .withName("tallestStudentInEachClassMotherHeightAvg")
                        .withDataType(doubleType)
                        .withGetterExpression(getModelName() + "::School.classes.tallestStudent.mother!avg(m | m.height)")
                        .withMemberType(MemberType.DERIVED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("tallestStudentsInEachClassMotherHeightAvg")
                        .withDataType(doubleType)
                        .withGetterExpression(getModelName() + "::School.classes.tallestStudents.mother!avg(m | m.height)")
                        .withMemberType(MemberType.DERIVED)
                        .build())
                .build();
        ActorType actor = newActorTypeBuilder().withName("actor").withPrincipal(ap).build();
        useTransferObjectType(ap).withActorType(actor).build();

        model.getElements().addAll(Arrays.asList(
                stringType, integerType, doubleType, genderType, school, clazz, person, student, ap, actor
        ));
        return model;
    }

    JudoRuntimeFixture runtimeFixture;

    @BeforeEach
    public void initFixture(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        this.runtimeFixture = runtimeFixture;
        this.runtimeFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.dropDatabase();
    }

    private UUID createSchool(String name) {
        EClass schoolType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".School").get();
        Payload school = runtimeFixture.getDao().create(schoolType, map("name", name), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        UUID id = school.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        return id;
    }

    private UUID createClass(String name, UUID schoolId) {
        EClass schoolType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".School").get();
        EReference schoolClasses = schoolType.getEAllReferences().stream().filter(r -> "classes".equals(r.getName())).findAny().get();
        Payload classPayload = runtimeFixture.getDao().createNavigationInstanceAt(schoolId, schoolClasses, map("name", name), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        UUID id = classPayload.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("Created class {} of school {} with ID: {}", new Object[]{name, schoolId, id});
        return id;
    }

    private UUID createStudent(String name, double height, UUID classId) {
        EClass classType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Class").get();
        EReference classStudents = classType.getEAllReferences().stream().filter(r -> "students".equals(r.getName())).findAny().get();
        Payload studentPayload = runtimeFixture.getDao().createNavigationInstanceAt(classId, classStudents, map("name", name, "height", height), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build());
        UUID id = studentPayload.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("Created student {} of class {} with ID: {}", new Object[]{name, classId, id});
        return id;
    }

    private UUID createParent(String name, double height, int gender, UUID studentId) {
        EClass studentType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Student").get();
        EReference studentParents = studentType.getEAllReferences().stream().filter(r -> "parents".equals(r.getName())).findAny().get();
        Payload parentPayload = runtimeFixture.getDao().createNavigationInstanceAt(studentId, studentParents,
                map("name", name, "height", height, "gender", gender), DAO.QueryCustomizer.<UUID>builder()
                        .mask(Collections.emptyMap())
                        .build());
        UUID id = parentPayload.getAs(runtimeFixture.getIdProvider().getType(), runtimeFixture.getIdProvider().getName());
        log.debug("Created parent {} of student {} with ID: {}", new Object[]{name, studentId, id});
        return id;
    }

    @Test
    public void testDerivedHead() {
        UUID school1 = createSchool("School1");
        UUID class1 = createClass("Class1/School1", school1);
        UUID class2 = createClass("Class2/School1", school1);
        UUID student_1_1 = createStudent("Student1/Class1", 180, class1);
        UUID father_1_1 = createParent("Father/Student1", 190, 0, student_1_1);
        UUID mother_1_1 = createParent("Mother/Student1/Class1", 180, 1, student_1_1);
        UUID student_2_1 = createStudent("Student2/Class1", 170, class1);
        UUID mother_2_1 = createParent("Mother/Student2/Class1", 160, 1, student_2_1);
        UUID student_3_1 = createStudent("Student2/Class1", 180, class1);
        UUID mother_3_1 = createParent("Mother/Student3/Class1", 180, 1, student_3_1);
        UUID student_1_2 = createStudent("Student1/Class2", 160, class2);
        UUID mother_1_2 = createParent("Mother/Student1/Class2", 150, 1, student_1_2);

        EClass apType = runtimeFixture.getAsmUtils().getClassByFQName(MODEL_NAME + ".AP").get();
        EClass studentType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Student").get();
        EReference tallestStudentInEachClass = apType.getEAllReferences().stream().filter(r -> "tallestStudentInEachClass".equals(r.getName())).findAny().get();
        List<Payload> tallestStudentInEachClassList = runtimeFixture.getDao().getAllReferencedInstancesOf(tallestStudentInEachClass, studentType).stream()
                .collect(Collectors.toList());
        assertThat(tallestStudentInEachClassList, hasSize(2));
        Payload tallestStudentInEachClassMotherHeightAvg = runtimeFixture.getDao().getStaticData(apType.getEAllAttributes().stream().filter(a -> "tallestStudentInEachClassMotherHeightAvg".equals(a.getName())).findAny().get());
        Payload tallestStudentsInEachClassMotherHeightAvg = runtimeFixture.getDao().getStaticData(apType.getEAllAttributes().stream().filter(a -> "tallestStudentsInEachClassMotherHeightAvg".equals(a.getName())).findAny().get());
        assertThat(tallestStudentInEachClassMotherHeightAvg.getAs(Double.class, "tallestStudentInEachClassMotherHeightAvg"), is(165.0));
        assertThat(tallestStudentsInEachClassMotherHeightAvg.getAs(Double.class, "tallestStudentsInEachClassMotherHeightAvg"), is(170.0));

        EClass schoolType = runtimeFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".School").get();

        long allSchoolQueryStart = System.currentTimeMillis();
        Payload school1Payload = runtimeFixture.getDao().getAllOf(schoolType).get(0);
        log.debug("All school query time: {} ms", System.currentTimeMillis() - allSchoolQueryStart);
        log.debug("School1: \n {} ", school1Payload);
        Collection<Payload> tallestStudentMothers = school1Payload.getAsCollectionPayload("tallestStudentMothers");
        assertThat(tallestStudentMothers.size(), is(2));
        assertThat(tallestStudentMothers.stream().map(p -> p.getAs(Double.class, "height")).max(Double::compare).get(), is(180.0));

        Collection<Payload> tallestStudentsMothers = school1Payload.getAsCollectionPayload("tallestStudentsMothers");
        assertThat(tallestStudentsMothers.size(), is(3));
        assertThat(tallestStudentMothers.stream().map(p -> p.getAs(Double.class, "height")).min(Double::compare).get(), is(150.0));
    }
}
