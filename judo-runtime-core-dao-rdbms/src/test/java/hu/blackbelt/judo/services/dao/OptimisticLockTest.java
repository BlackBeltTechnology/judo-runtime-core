package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import hu.blackbelt.judo.services.dao.rdbms.executors.StatementExecutor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.UUID;
import java.util.function.Function;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class OptimisticLockTest {

    public static final String MODEL_NAME = "M";
    public static final String NAME = "name";
    public static final String CLASS = "class";

    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    @AfterEach
    public void teardown(final RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testSimpleOptimisticLock(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final EntityType person = newEntityTypeBuilder()
                .withName("Person")
                .withAttributes(newDataMemberBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(person)
                .withMapping(newMappingBuilder().withTarget(person).build())
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(stringType, integerType, booleanType, person)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass personType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Person").get();
        final Function<Payload, UUID> idExtractor = p -> p.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final OffsetDateTime ts1 = OffsetDateTime.now().truncatedTo(ChronoUnit.MICROS);

        final Payload p1 = daoFixture.getDao().create(personType, Payload.map(
                NAME, "Gipsz Jakab"
        ), null);
        final OffsetDateTime ts2 = OffsetDateTime.now();

        log.debug("Person #1: {}", p1);
        final UUID p1Id = idExtractor.apply(p1);
        final OffsetDateTime createdTs1 = p1.getAs(OffsetDateTime.class, StatementExecutor.ENTITY_CREATE_TIMESTAMP_MAP_KEY);

        assertThat(p1.getAs(Integer.class, StatementExecutor.ENTITY_VERSION_MAP_KEY), equalTo(1));
        assertTrue((ts1.isBefore(createdTs1) || ts1.isEqual(createdTs1)) && (ts2.isAfter(createdTs1) || ts2.isEqual(createdTs1)));

        // test update without version
        final Payload p2 = daoFixture.getDao().update(personType, Payload.map(
                daoFixture.getIdProvider().getName(), p1Id,
                NAME, "Teszt Elek"
        ), null);
        final OffsetDateTime ts3 = OffsetDateTime.now();

        log.debug("Person #2: {}", p2);
        final OffsetDateTime updatedTs2 = p2.getAs(OffsetDateTime.class, StatementExecutor.ENTITY_UPDATE_TIMESTAMP_MAP_KEY);

        assertThat(p2.getAs(Integer.class, StatementExecutor.ENTITY_VERSION_MAP_KEY), equalTo(2));
        assertThat(p2.getAs(String.class, NAME), equalTo("Teszt Elek"));
        assertTrue((ts2.isBefore(updatedTs2) || ts2.isEqual(updatedTs2)) && (ts3.isAfter(updatedTs2) || ts3.isEqual(updatedTs2)));

        // test update with valid version
        final Payload p3 = daoFixture.getDao().update(personType, Payload.map(
                daoFixture.getIdProvider().getName(), p1Id,
                NAME, "Gipsz Jakab",
                StatementExecutor.ENTITY_VERSION_MAP_KEY, 2
        ), null);
        final OffsetDateTime ts4 = OffsetDateTime.now();

        log.debug("Person #3: {}", p3);
        final OffsetDateTime updatedTs3 = p3.getAs(OffsetDateTime.class, StatementExecutor.ENTITY_UPDATE_TIMESTAMP_MAP_KEY);

        assertThat(p3.getAs(Integer.class, StatementExecutor.ENTITY_VERSION_MAP_KEY), equalTo(3));
        assertThat(p3.getAs(String.class, NAME), equalTo("Gipsz Jakab"));
        assertTrue((ts3.isBefore(updatedTs3) || ts3.isEqual(updatedTs3)) && (ts4.isAfter(updatedTs3) || ts4.isEqual(updatedTs3)));

        // test update with invalid version
        assertThrows(IllegalArgumentException.class, () -> daoFixture.getDao().update(personType, Payload.map(
                daoFixture.getIdProvider().getName(), p1Id,
                NAME, "Teszt Elek",
                StatementExecutor.ENTITY_VERSION_MAP_KEY, 2
        ), null));

        // ensure update is not executed
        final Payload p4 = daoFixture.getDao().getByIdentifier(personType, p1Id).get();
        log.debug("Person #4: {}", p4);
        final OffsetDateTime updatedTs4 = p4.getAs(OffsetDateTime.class, StatementExecutor.ENTITY_UPDATE_TIMESTAMP_MAP_KEY);
        assertThat(p4.getAs(Integer.class, StatementExecutor.ENTITY_VERSION_MAP_KEY), equalTo(3));
        assertThat(p4.getAs(String.class, NAME), equalTo("Gipsz Jakab"));
        assertTrue((ts3.isBefore(updatedTs4) || ts3.isEqual(updatedTs4)) && (ts4.isAfter(updatedTs4) || ts4.isEqual(updatedTs4)));
    }

    @Test
    public void testInheritance(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();

        final EntityType person = newEntityTypeBuilder()
                .withName("Person")
                .withAttributes(newDataMemberBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(person)
                .withMapping(newMappingBuilder().withTarget(person).build())
                .build();

        final EntityType student = newEntityTypeBuilder()
                .withName("Student")
                .withAttributes(newDataMemberBuilder()
                        .withName(CLASS)
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();
        useEntityType(student)
                .withGeneralizations(newGeneralizationBuilder().withTarget(person).build())
                .withMapping(newMappingBuilder().withTarget(student).build())
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(stringType, integerType, booleanType, person, student)
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass personType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Person").get();
        final EClass studentType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Student").get();
        final Function<Payload, UUID> idExtractor = p -> p.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        final Payload p1 = daoFixture.getDao().create(studentType, Payload.map(
                NAME, "Gipsz Jakab"
        ), null);
        final UUID p1Id = idExtractor.apply(p1);
        assertThat(p1.getAs(Integer.class, StatementExecutor.ENTITY_VERSION_MAP_KEY), equalTo(1));
        final Payload p2 = daoFixture.getDao().create(studentType, Payload.map(
                NAME, "Teszt Elek",
                CLASS, "9/C"
        ), null);
        final UUID p2Id = idExtractor.apply(p2);
        assertThat(p2.getAs(Integer.class, StatementExecutor.ENTITY_VERSION_MAP_KEY), equalTo(1));

        final OffsetDateTime ts1 = OffsetDateTime.now();

        p1.put(NAME, "Jakab Gipsz");
        daoFixture.getDao().update(studentType, p1, null);

        p2.put(NAME, "Elek Teszt");
        daoFixture.getDao().update(personType, p2, null);

        final OffsetDateTime ts2 = OffsetDateTime.now();

        final Payload p1UpdatedAsStudent = daoFixture.getDao().getByIdentifier(studentType, p1Id).get();
        final Payload p1UpdatedAsPerson = daoFixture.getDao().getByIdentifier(personType, p1Id).get();
        final OffsetDateTime p1UpdatedAsStudentTs = p1UpdatedAsStudent.getAs(OffsetDateTime.class, StatementExecutor.ENTITY_UPDATE_TIMESTAMP_MAP_KEY);
        final OffsetDateTime p1UpdatedAsPersonTs = p1UpdatedAsPerson.getAs(OffsetDateTime.class, StatementExecutor.ENTITY_UPDATE_TIMESTAMP_MAP_KEY);

        log.debug("P1 as student: {}", p1UpdatedAsStudent);
        log.debug("P1 as person: {}", p1UpdatedAsPerson);
        assertThat(p1UpdatedAsStudent.getAs(Integer.class, StatementExecutor.ENTITY_VERSION_MAP_KEY), equalTo(2));
        assertThat(p1UpdatedAsPerson.getAs(Integer.class, StatementExecutor.ENTITY_VERSION_MAP_KEY), equalTo(2));
        assertTrue((p1UpdatedAsStudentTs.isBefore(ts2) || p1UpdatedAsStudentTs.isEqual(ts2)) && (p1UpdatedAsStudentTs.isAfter(ts1) || p1UpdatedAsStudentTs.isEqual(ts1)));
        assertTrue((p1UpdatedAsPersonTs.isBefore(ts2) || p1UpdatedAsPersonTs.isEqual(ts2)) && (p1UpdatedAsPersonTs.isAfter(ts1) || p1UpdatedAsPersonTs.isEqual(ts1)));

        final Payload p2UpdatedAsStudent = daoFixture.getDao().getByIdentifier(studentType, p2Id).get();
        final Payload p2UpdatedAsPerson = daoFixture.getDao().getByIdentifier(personType, p2Id).get();
        final OffsetDateTime p2UpdatedAsStudentTs = p2UpdatedAsStudent.getAs(OffsetDateTime.class, StatementExecutor.ENTITY_UPDATE_TIMESTAMP_MAP_KEY);
        final OffsetDateTime p2UpdatedAsPersonTs = p2UpdatedAsPerson.getAs(OffsetDateTime.class, StatementExecutor.ENTITY_UPDATE_TIMESTAMP_MAP_KEY);

        log.debug("P2 as student: {}", p2UpdatedAsStudent);
        log.debug("P2 as person: {}", p2UpdatedAsPerson);
        assertThat(p2UpdatedAsStudent.getAs(Integer.class, StatementExecutor.ENTITY_VERSION_MAP_KEY), equalTo(2));
        assertThat(p2UpdatedAsPerson.getAs(Integer.class, StatementExecutor.ENTITY_VERSION_MAP_KEY), equalTo(2));
        assertTrue((p2UpdatedAsStudentTs.isBefore(ts2) || p2UpdatedAsStudentTs.isEqual(ts2)) && (p2UpdatedAsStudentTs.isAfter(ts1) || p2UpdatedAsStudentTs.isEqual(ts1)));
        assertTrue((p2UpdatedAsPersonTs.isBefore(ts2) || p2UpdatedAsPersonTs.isEqual(ts2)) && (p2UpdatedAsPersonTs.isAfter(ts1) || p2UpdatedAsPersonTs.isEqual(ts1)));
    }
}
