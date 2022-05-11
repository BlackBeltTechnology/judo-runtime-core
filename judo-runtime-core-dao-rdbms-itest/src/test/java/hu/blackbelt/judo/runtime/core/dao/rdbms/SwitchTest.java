package hu.blackbelt.judo.runtime.core.dao.rdbms;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.measure.DurationType;
import hu.blackbelt.judo.meta.esm.measure.Measure;
import hu.blackbelt.judo.meta.esm.measure.Unit;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.type.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

import static hu.blackbelt.judo.meta.esm.measure.util.builder.MeasureBuilders.newDurationUnitBuilder;
import static hu.blackbelt.judo.meta.esm.measure.util.builder.MeasureBuilders.newMeasureBuilder;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newPackageBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class SwitchTest {

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

        final EntityType tester = newEntityTypeBuilder()
                .withName("Tester")
                .withAttributes(newDataMemberBuilder()
                        .withName("rString")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("oString")
                        .withDataType(stringType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("string")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.rBoolean ? self.rString : self.oString")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("constantString")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.rBoolean ? 'X' : 'Y'")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("rInteger")
                        .withDataType(integerType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("oInteger")
                        .withDataType(integerType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("integer")
                        .withDataType(integerType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.rBoolean ? self.rInteger : self.oInteger")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("rDouble")
                        .withDataType(doubleType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("oDouble")
                        .withDataType(doubleType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("double")
                        .withDataType(doubleType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.rBoolean ? self.rDouble : self.oDouble")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("rBoolean")
                        .withDataType(booleanType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("oBoolean")
                        .withDataType(booleanType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("unknownCondition")
                        .withDataType(stringType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.oBoolean ? 'INVALID' : self.rString")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("rDate")
                        .withDataType(dateType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("oDate")
                        .withDataType(dateType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("date")
                        .withDataType(dateType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.rBoolean ? self.rDate : self.oDate")
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("rTimestamp")
                        .withDataType(timestampType)
                        .withRequired(true)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("oTimestamp")
                        .withDataType(timestampType)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("timestamp")
                        .withDataType(timestampType)
                        .withMemberType(MemberType.DERIVED)
                        .withGetterExpression("self.rBoolean ? self.rTimestamp : self.oTimestamp")
                        .build())
                .build();
        useEntityType(tester)
                .withMapping(newMappingBuilder().withTarget(tester).build())
                .build();

        final Model model = newModelBuilder()
                .withName(getModelName())
                .withElements(newPackageBuilder()
                        .withName("type")
                        .withElements(Arrays.asList(stringType, integerType, doubleType, booleanType, dateType, timestampType, time))
                        .build())
                .withElements(newPackageBuilder()
                        .withName("entity")
                        .withElements(tester)
                        .build())
                .build();

        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");

        final EClass testerType = daoFixture.getAsmUtils().all(EClass.class)
                .filter(c -> (DTO_PACKAGE + ".entity.Tester").equals(AsmUtils.getClassifierFQName(c)))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Default transfer object type of Tester not found in ASM model"));

        final String stringValue = "STRING";
        final Integer integerValue = 10;
        final Double doubleValue = 3.1415;
        final LocalDate dateValue = LocalDate.of(2020, 10, 20);
        final OffsetDateTime timestampValue = OffsetDateTime.of(2020, 10, 20, 16, 30, 05, 0, ZoneOffset.UTC);

        final Payload tester1 = daoFixture.getDao().create(testerType, Payload.map(
                "rString", stringValue,
                "rInteger", integerValue,
                "rDouble", doubleValue,
                "rBoolean", true,
                "rDate", dateValue,
                "rTimestamp", timestampValue
        ), null);

        log.debug("Tester #1: {}", tester1);

        assertThat(tester1.getAs(String.class, "constantString"), equalTo("X"));
        assertThat(tester1.getAs(String.class, "string"), equalTo(stringValue));
        assertThat(tester1.getAs(Integer.class, "integer"), equalTo(integerValue));
        assertThat(tester1.getAs(Double.class, "double"), equalTo(doubleValue));
        assertThat(tester1.getAs(LocalDate.class, "date"), equalTo(dateValue));
        assertThat(tester1.getAs(OffsetDateTime.class, "timestamp"), equalTo(timestampValue));
        assertThat(tester1.getAs(String.class, "unknownCondition"), equalTo(stringValue));

        final Payload tester2 = daoFixture.getDao().create(testerType, Payload.map(
                "rString", stringValue,
                "rInteger", integerValue,
                "rDouble", doubleValue,
                "rBoolean", false,
                "rDate", dateValue,
                "rTimestamp", timestampValue
        ), null);

        log.debug("Tester #2: {}", tester2);

        assertThat(tester2.getAs(String.class, "constantString"), equalTo("Y"));
        assertThat(tester2.getAs(String.class, "string"), nullValue());
        assertThat(tester2.getAs(Integer.class, "integer"), nullValue());
        assertThat(tester2.getAs(Double.class, "double"), nullValue());
        assertThat(tester2.getAs(LocalDate.class, "date"), nullValue());
        assertThat(tester2.getAs(OffsetDateTime.class, "timestamp"), nullValue());
        assertThat(tester2.getAs(String.class, "unknownCondition"), equalTo(stringValue));
    }
}
