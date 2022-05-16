package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.runtime.core.dao.rdbms.custom.Gps;
import hu.blackbelt.judo.runtime.core.dao.rdbms.custom.StringToGpsConverter;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collections;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newDataMemberBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newTransferObjectTypeBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class EnvironmentVariableTest {

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    public void currentTimestamp(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        OffsetDateTime startTimestamp = OffsetDateTime.now();
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject")
                .withStaticData("Timestamp", "currentTimestamp", "demo::types::TimeStamp!getVariable('SYSTEM', 'current_timestamp')")
                .withStaticData("Timestamp", "timeNow", "demo::types::TimeStamp!now()");
        Model model = modelBuilder.build();
        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
        EClass eClass = runtimeFixture.getAsmUtils().getClassByFQName("demo.services.TransferObject").get();
        Payload payload = runtimeFixture.getDao().getStaticData(getAttribute(eClass, "currentTimestamp"));
        OffsetDateTime returnedTimestamp = payload.getAs(OffsetDateTime.class, "currentTimestamp");
        payload = runtimeFixture.getDao().getStaticData(getAttribute(eClass, "timeNow"));
        OffsetDateTime returnedTimeNow = payload.getAs(OffsetDateTime.class, "timeNow");
        OffsetDateTime endTimestamp = OffsetDateTime.now();
        assertThat(returnedTimestamp, greaterThan(startTimestamp));
        assertThat(returnedTimestamp, lessThan(endTimestamp));
        assertThat(returnedTimeNow, greaterThan(startTimestamp));
        assertThat(returnedTimeNow, lessThan(endTimestamp));
    }

    @Test
    public void currentTime(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        LocalTime now = LocalTime.now();
        LocalTime startTime = LocalTime.of(now.getHour(), now.getMinute(), now.getSecond());
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject")
                .withStaticData("Time", "currentTime", "demo::types::Time!getVariable('SYSTEM', 'current_time')")
                .withStaticData("Time", "timeNow", "demo::types::Time!now()");
        Model model = modelBuilder.build();
        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
        EClass eClass = runtimeFixture.getAsmUtils().getClassByFQName("demo.services.TransferObject").get();
        Payload payload = runtimeFixture.getDao().getStaticData(getAttribute(eClass, "currentTime"));
        LocalTime returnedTime = payload.getAs(LocalTime.class, "currentTime");
        payload = runtimeFixture.getDao().getStaticData(getAttribute(eClass, "timeNow"));
        LocalTime returnedTimeNow = payload.getAs(LocalTime.class, "timeNow");
        LocalTime endTime = LocalTime.now();
        assertThat(returnedTime, not(lessThan((startTime))));
        assertThat(returnedTime, not(greaterThan(endTime)));
        assertThat(returnedTimeNow, not(lessThan(startTime)));
        assertThat(returnedTimeNow, not(greaterThan(endTime)));
    }

    @Test
    public void currentDate(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        LocalDate startDate = LocalDate.now();
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject")
                .withStaticData("Date", "currentDate", "demo::types::Date!getVariable('SYSTEM', 'current_date')")
                .withStaticData("Date", "dateNow", "demo::types::Date!now()");
        Model model = modelBuilder.build();
        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
        EClass eClass = runtimeFixture.getAsmUtils().getClassByFQName("demo.services.TransferObject").get();
        Payload payload = runtimeFixture.getDao().getStaticData(getAttribute(eClass, "currentDate"));
        LocalDate returnedCurrentDate = payload.getAs(LocalDate.class, "currentDate");
        payload = runtimeFixture.getDao().getStaticData(getAttribute(eClass, "dateNow"));
        LocalDate returnedDateNow = payload.getAs(LocalDate.class, "dateNow");
        LocalDate endDate = LocalDate.now();
        assertThat(returnedCurrentDate, greaterThanOrEqualTo(startDate));
        assertThat(returnedCurrentDate, lessThanOrEqualTo(endDate));
        assertThat(returnedDateNow, greaterThanOrEqualTo(startDate));
        assertThat(returnedDateNow, lessThanOrEqualTo(endDate));
    }

    @Test
    public void systemAndEnvironmentProperties(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject")
                .withStaticData("Integer", "integer", "demo::types::Integer!getVariable('ENVIRONMENT', 'integer')")
                .withStaticData("Boolean", "boolean1", "demo::types::Boolean!getVariable('ENVIRONMENT', 'boolean1')")
                .withStaticData("Boolean", "boolean2", "demo::types::Boolean!getVariable('ENVIRONMENT', 'boolean2')")
                .withStaticData("String", "string", "demo::types::String!getVariable('ENVIRONMENT', 'string')")
                .withStaticData("String", "stringUndefined", "demo::types::String!getVariable('ENVIRONMENT', 'stringUndefined')")
                .withStaticData("Double", "double", "demo::types::Double!getVariable('ENVIRONMENT', 'double')")
                .withStaticData("Long", "long", "demo::types::Long!getVariable('ENVIRONMENT', 'long')")
                .withStaticData("MassStoredInKilograms", "mass", "demo::types::measured::MassStoredInKilograms!getVariable('ENVIRONMENT', 'mass')")
                .withStaticData("Country", "country", "demo::types::Countries!getVariable('ENVIRONMENT', 'country')")
                .withStaticData("Country", "countryUndefined", "demo::types::Countries!getVariable('ENVIRONMENT', 'countryUndefined')")
                .withStaticData("Gps", "gps", "demo::types::GPS!getVariable('ENVIRONMENT', 'gps')")
                .withStaticData("Date", "date", "demo::types::Date!getVariable('ENVIRONMENT', 'date')")
                .withStaticData("Timestamp", "timestamp", "demo::types::TimeStamp!getVariable('ENVIRONMENT', 'timestamp')")
                .withStaticData("Time", "time", "demo::types::Time!getVariable('ENVIRONMENT', 'time')")
        ;

        System.setProperty("integer", "1");
        System.setProperty("boolean1", "true");
        System.setProperty("boolean2", "TRUE");
        System.setProperty("string", "foo");
        System.setProperty("double", "3.1415926535");
        System.setProperty("long", "123456789012345678");
        System.setProperty("country", "AT");
        System.setProperty("mass", "42");
        System.setProperty("date", "2020-11-19");
        System.setProperty("timestamp", "2020-11-19T16:38:00+00:00");
        System.setProperty("time", "16:38:01");
        System.setProperty("gps", "47.510746,19.0346693");

        Model model = modelBuilder.build();
        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
        EDataType gpsType = runtimeFixture.getAsmUtils().resolve("demo.types.GPS").map(dataType -> (EDataType) dataType).get();
        runtimeFixture.getDataTypeManager().registerCustomType(gpsType, Gps.class.getName(), Collections.singleton(new StringToGpsConverter()));

        EClass eClass = runtimeFixture.getAsmUtils().getClassByFQName("demo.services.TransferObject").get();
        assertData(runtimeFixture, eClass, Integer.class, "integer", 1);
        assertData(runtimeFixture, eClass, Boolean.class, "boolean1", true);
        assertData(runtimeFixture, eClass, Boolean.class, "boolean2", true);
        assertData(runtimeFixture, eClass, String.class, "string", "foo");
        assertData(runtimeFixture, eClass, Double.class, "double", 3.1415926535);
        assertData(runtimeFixture, eClass, Long.class, "long", 123456789012345678L);
        assertData(runtimeFixture, eClass, Integer.class, "country", 1);
        assertData(runtimeFixture, eClass, Double.class, "mass", 42.0);
        assertData(runtimeFixture, eClass, String.class, "stringUndefined", nullValue());
        assertData(runtimeFixture, eClass, Integer.class, "countryUndefined", nullValue());
        assertData(runtimeFixture, eClass, String.class, "gps", "47.510746,19.0346693");
        assertData(runtimeFixture, eClass, OffsetDateTime.class, "timestamp", OffsetDateTime.parse("2020-11-19T16:38:00+00:00"));
        assertData(runtimeFixture, eClass, LocalDate.class, "date", LocalDate.of(2020, 11, 19));
        assertData(runtimeFixture, eClass, LocalTime.class, "time", LocalTime.of(16, 38, 01));
    }

    @Test
    public void testUsingVariablesToFilter(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity")
                .withAttribute("String", "attribute")
        ;

        modelBuilder.addUnmappedTransferObject("TransferObject")
                .withStaticNavigation("Entity", "filteredInstances", "demo::entities::Entity!filter(e | e.attribute == demo::types::String!getVariable('ENVIRONMENT', 'ATTRIBUTE_FILTER'))", cardinality(0, -1))
                .withStaticNavigation("Entity", "nullIsNotComparableInstances", "demo::entities::Entity!filter(e | e.attribute == demo::types::String!getVariable('ENVIRONMENT', 'NOT_DEFINED'))", cardinality(0, -1))
                .withStaticNavigation("Entity", "allInstances", "demo::entities::Entity!filter(e | demo::types::String!getVariable('ENVIRONMENT', 'ATTRIBUTE_FILTER')!isDefined())", cardinality(0, -1))
                .withStaticNavigation("Entity", "noInstances", "demo::entities::Entity!filter(e | demo::types::String!getVariable('ENVIRONMENT', 'NOT_DEFINED')!isDefined())", cardinality(0, -1))
                .withStaticNavigation("Entity", "allInstancesByVariableValue", "demo::entities::Entity!filter(e | 'X' == demo::types::String!getVariable('ENVIRONMENT', 'ATTRIBUTE_FILTER'))", cardinality(0, -1))
                .withStaticNavigation("Entity", "noInstancesByVariableValue", "demo::entities::Entity!filter(e | 'X' == demo::types::String!getVariable('ENVIRONMENT', 'NOT_DEFINED'))", cardinality(0, -1))
        ;

        System.setProperty("ATTRIBUTE_FILTER", "X");

        Model model = modelBuilder.build();
        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        EClass entityType = runtimeFixture.getAsmUtils().getClassByFQName("demo._default_transferobjecttypes.entities.Entity").get();
        Payload e1 = runtimeFixture.getDao().create(entityType, Payload.map("attribute", "X"), null);
        e1.remove("__$created");
        Payload e2 = runtimeFixture.getDao().create(entityType, Payload.map("attribute", "Y"), null);
        e2.remove("__$created");
        Payload e3 = runtimeFixture.getDao().create(entityType, Payload.empty(), null);
        e3.remove("__$created");

        EClass eClass = runtimeFixture.getAsmUtils().getClassByFQName("demo.services.TransferObject").get();
        Payload result = runtimeFixture.getDao().getStaticFeatures(eClass);
        log.debug("Result of filtering by variables: {}", result);
        assertThat(result.getAsCollectionPayload("filteredInstances"), equalTo(Collections.singletonList(e1)));
        assertThat(result.getAsCollectionPayload("nullIsNotComparableInstances"), equalTo(Collections.emptyList()));
        assertThat(ImmutableSet.copyOf(result.getAsCollectionPayload("allInstancesByVariableValue")), equalTo(ImmutableSet.of(e1, e2, e3)));
        assertThat(result.getAsCollectionPayload("noInstancesByVariableValue"), equalTo(Collections.emptyList()));
        assertThat(ImmutableSet.copyOf(result.getAsCollectionPayload("allInstances")), equalTo(ImmutableSet.of(e1, e2, e3)));
        assertThat(result.getAsCollectionPayload("noInstances"), equalTo(Collections.emptyList()));
    }

    @Test
    public void testCheckingEnumVariableIsDefined(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        BooleanType booleanType = newBooleanTypeBuilder()
                .withName("Boolean")
                .build();

        hu.blackbelt.judo.meta.esm.namespace.Model model = newModelBuilder()
                .withName("M")
                .withElements(booleanType)
                .withElements(newEnumerationTypeBuilder()
                        .withName("Country")
                        .withMembers(newEnumerationMemberBuilder()
                                .withName("HU")
                                .withOrdinal(0)
                                .build())
                        .withMembers(newEnumerationMemberBuilder()
                                .withName("AT")
                                .withOrdinal(1)
                                .build())
                        .withMembers(newEnumerationMemberBuilder()
                                .withName("RO")
                                .withOrdinal(2)
                                .build())
                        .build())
                .withElements(newTransferObjectTypeBuilder()
                        .withName("Tester")
                        .withAttributes(newDataMemberBuilder()
                                .withName("hungary")
                                .withDataType(booleanType)
                                .withMemberType(MemberType.DERIVED)
                                .withGetterExpression("M::Country#HU == M::Country!getVariable('ENVIRONMENT', 'COUNTRY')")
                                .build())
                        .withAttributes(newDataMemberBuilder()
                                .withName("austria")
                                .withDataType(booleanType)
                                .withMemberType(MemberType.DERIVED)
                                .withGetterExpression("M::Country#AT == M::Country!getVariable('ENVIRONMENT', 'COUNTRY')")
                                .build())
                        .withAttributes(newDataMemberBuilder()
                                .withName("unknown")
                                .withDataType(booleanType)
                                .withMemberType(MemberType.DERIVED)
                                .withGetterExpression("M::Country#HU == M::Country!getVariable('ENVIRONMENT', 'NOT_DEFINED')")
                                .build())
                        .withAttributes(newDataMemberBuilder()
                                .withName("defined")
                                .withDataType(booleanType)
                                .withMemberType(MemberType.DERIVED)
                                .withGetterExpression("M::Country!getVariable('ENVIRONMENT', 'COUNTRY')!isDefined()")
                                .build())
                        .withAttributes(newDataMemberBuilder()
                                .withName("undefined")
                                .withDataType(booleanType)
                                .withMemberType(MemberType.DERIVED)
                                .withGetterExpression("M::Country!getVariable('ENVIRONMENT', 'NOT_DEFINED')!isUndefined()")
                                .build())
                        .build())
                .build();

        System.setProperty("COUNTRY", "HU");

        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        EClass eClass = runtimeFixture.getAsmUtils().getClassByFQName("M.Tester").get();
        Payload result = runtimeFixture.getDao().getStaticFeatures(eClass);
        log.debug("Result of testCheckingEnumVariableIsDefined: {}", result);

        assertThat(result, equalTo(Payload.map(
                "hungary", true,
                "austria", false,
                "unknown", null,
                "defined", true,
                "undefined", true
        )));
    }

    private void assertData(JudoRuntimeFixture runtimeFixture, EClass eClass, Class<?> resultClass, String attribute, Object expectedValue) {
        assertData(runtimeFixture, eClass, resultClass, attribute, is(expectedValue));
    }

    private void assertData(JudoRuntimeFixture runtimeFixture, EClass eClass, Class<?> resultClass, String attribute, Matcher<Object> matcher) {
        Payload payload = runtimeFixture.getDao().getStaticData(getAttribute(eClass, attribute));
        assertThat(payload.getAs(resultClass, attribute), matcher);
    }

    private EAttribute getAttribute(EClass eClass, String currentDate) {
        return eClass.getEAttributes().stream().filter(a -> a.getName().equals(currentDate)).findAny().get();
    }

}
