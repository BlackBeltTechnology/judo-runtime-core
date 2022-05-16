package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newDataMemberBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newTransferObjectTypeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class ParameterTest {

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    public void systemAndEnvironmentPropertiesWithOperation(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        PsmTestModelBuilder modelBuilder = new PsmTestModelBuilder();

        modelBuilder.addUnmappedTransferObject("TransferObject")
                .withStaticData("Integer", "integer", "demo::types::Integer!getVariable('ENVIRONMENT', 'integer') + 1")
                .withStaticData("Boolean", "boolean1", "not demo::types::Boolean!getVariable('ENVIRONMENT', 'boolean1')")
                .withStaticData("Boolean", "boolean2", "not demo::types::Boolean!getVariable('ENVIRONMENT', 'boolean2')")
                .withStaticData("String", "string", "demo::types::String!getVariable('ENVIRONMENT', 'string') + 'postfix'")
                .withStaticData("String", "stringUndefined", "demo::types::String!getVariable('ENVIRONMENT', 'stringUndefined') + 'postfix'")
                .withStaticData("Double", "double", "demo::types::Double!getVariable('ENVIRONMENT', 'double') + 1.0")
                .withStaticData("Long", "long", "demo::types::Long!getVariable('ENVIRONMENT', 'long') + 1")
                .withStaticData("MassStoredInKilograms", "mass", "demo::types::measured::MassStoredInKilograms!getVariable('ENVIRONMENT', 'mass') + 1[kg]")
                .withStaticData("Date", "date", "demo::types::Date!getVariable('ENVIRONMENT', 'date') + 1[day]")
                .withStaticData("Timestamp", "timestamp", "demo::types::TimeStamp!getVariable('ENVIRONMENT', 'timestamp') + 720[minute]")
                .withStaticData("Time", "time", "demo::types::Time!getVariable('ENVIRONMENT', 'time') + 60[minute]")
        ;

        System.setProperty("integer", "1");
        System.setProperty("boolean1", "true");
        System.setProperty("boolean2", "TRUE");
        System.setProperty("string", "foo");
        System.setProperty("double", "3.1415926535");
        System.setProperty("long", "123456789012345678");
        System.setProperty("mass", "42");
        System.setProperty("date", "2020-11-19");
        System.setProperty("timestamp", "2020-11-19T16:38:00+00:00");
        System.setProperty("time", "16:38:01");

        Model model = modelBuilder
                .build();
        runtimeFixture.init(model, datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");

        EClass eClass = runtimeFixture.getAsmUtils().getClassByFQName("demo.services.TransferObject").get();
        assertData(runtimeFixture, eClass, Integer.class, "integer", 2);
        assertData(runtimeFixture, eClass, Boolean.class, "boolean1", false);
        assertData(runtimeFixture, eClass, Boolean.class, "boolean2", false);
        assertData(runtimeFixture, eClass, String.class, "string", "foopostfix");
        assertData(runtimeFixture, eClass, Double.class, "double", 4.1415926535);
        assertData(runtimeFixture, eClass, Long.class, "long", 123456789012345679L);
        assertData(runtimeFixture, eClass, Double.class, "mass", 43.0);
        assertData(runtimeFixture, eClass, String.class, "stringUndefined", nullValue());
        assertData(runtimeFixture, eClass, OffsetDateTime.class, "timestamp", OffsetDateTime.parse("2020-11-20T04:38:00+00:00"));
        assertData(runtimeFixture, eClass, LocalDate.class, "date", LocalDate.of(2020, 11, 20));
        assertData(runtimeFixture, eClass, LocalTime.class, "time", LocalTime.of(17, 38, 01));
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
