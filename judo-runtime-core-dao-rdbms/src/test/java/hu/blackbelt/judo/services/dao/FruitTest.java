package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.data.Attribute;
import hu.blackbelt.judo.meta.psm.data.EntityType;
import hu.blackbelt.judo.meta.psm.derived.DataProperty;
import hu.blackbelt.judo.meta.psm.derived.ExpressionDialect;
import hu.blackbelt.judo.meta.psm.measure.DurationType;
import hu.blackbelt.judo.meta.psm.measure.DurationUnit;
import hu.blackbelt.judo.meta.psm.measure.Measure;
import hu.blackbelt.judo.meta.psm.measure.MeasuredType;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.service.MappedTransferObjectType;
import hu.blackbelt.judo.meta.psm.type.*;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.UUID;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.psm.data.util.builder.DataBuilders.*;
import static hu.blackbelt.judo.meta.psm.derived.util.builder.DerivedBuilders.newDataExpressionTypeBuilder;
import static hu.blackbelt.judo.meta.psm.derived.util.builder.DerivedBuilders.newDataPropertyBuilder;
import static hu.blackbelt.judo.meta.psm.measure.util.builder.MeasureBuilders.*;
import static hu.blackbelt.judo.meta.psm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.psm.service.util.builder.ServiceBuilders.newMappedTransferObjectTypeBuilder;
import static hu.blackbelt.judo.meta.psm.service.util.builder.ServiceBuilders.newTransferAttributeBuilder;
import static hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class FruitTest {
    public static final String MODEL_NAME = "fruittest";
    public static final String FRUIT_ENTITY = "Fruit";
    public static final String FRUIT_DTO = "FruitDTO";

    public static final String NAME = "name";

    public static final String PLANTED_ON = "plantedOn";
    public static final String RIPENING_TIME = "ripeningTime";
    public static final String CALCULATED_RIPENING_TIME = "calculatedRipeningTimeInHours";
    public static final String GATHERED_ON = "gatheredOn";
    public static final String PRODUCED_ON = "producedOn";
    public static final String PRODUCED_IN_2020 = "producedIn2020";
    public static final String QUALITY_DURATION = "qualityDuration";
    public static final String CALCULATED_QUALITY_DURATION = "calculatedQualityDurationInWeeks";
    public static final String BEST_BEFORE = "bestBefore";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getPsmModel() {
        final Measure time = newMeasureBuilder().withName("Time").withSymbol("t").build();
        final DurationUnit nanosecond = newDurationUnitBuilder().withName("nanosecond").withSymbol("ns").withRateDividend(0.001).withRateDivisor(1000000).withUnitType(DurationType.NANOSECOND).build();
        final DurationUnit microsecond = newDurationUnitBuilder().withName("microsecond").withSymbol("μs").withRateDividend(1).withRateDivisor(1000000).withUnitType(DurationType.MICROSECOND).build();
        final DurationUnit millisecond = newDurationUnitBuilder().withName("millisecond").withSymbol("ms").withRateDividend(1).withRateDivisor(1000).withUnitType(DurationType.MILLISECOND).build();
        final DurationUnit second = newDurationUnitBuilder().withName("second").withSymbol("s").withRateDividend(1).withRateDivisor(1).withUnitType(DurationType.SECOND).build();
        final DurationUnit minute = newDurationUnitBuilder().withName("minute").withSymbol("m").withRateDividend(60).withRateDivisor(1).withUnitType(DurationType.MINUTE).build();
        final DurationUnit hour = newDurationUnitBuilder().withName("hour").withSymbol("h").withRateDividend(3600).withRateDivisor(1).withUnitType(DurationType.HOUR).build();
        final DurationUnit day = newDurationUnitBuilder().withName("day").withSymbol("d").withRateDividend(86400).withRateDivisor(1).withUnitType(DurationType.DAY).build();
        final DurationUnit week = newDurationUnitBuilder().withName("week").withSymbol("w").withRateDividend(604800).withRateDivisor(1).withUnitType(DurationType.WEEK).build();
        useMeasure(time).withUnits(Arrays.asList(nanosecond, microsecond, millisecond, second, minute, hour, day, week)).build();

        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(10).build();
        final NumericType numericType = newNumericTypeBuilder().withName("Numeric").withPrecision(10).withScale(5).build();
        final DateType dateType = newDateTypeBuilder().withName("Date").build();
        final TimestampType timestampType = newTimestampTypeBuilder().withName("Timestamp").withBaseUnit(DurationType.MICROSECOND).build();
        final MeasuredType secondType = newMeasuredTypeBuilder().withName("Second").withPrecision(15).withScale(4).withStoreUnit(second).build();
        final MeasuredType hourType = newMeasuredTypeBuilder().withName("Hour").withPrecision(15).withScale(4).withStoreUnit(hour).build();
        final MeasuredType dayType = newMeasuredTypeBuilder().withName("Day").withPrecision(15).withScale(4).withStoreUnit(day).build();
        final MeasuredType weekType = newMeasuredTypeBuilder().withName("Week").withPrecision(15).withScale(4).withStoreUnit(week).build();

        Attribute fruitName = newAttributeBuilder()
                .withName(NAME)
                .withDataType(stringType)
                .withRequired(true)
                .build();
        Attribute plantedOn = newAttributeBuilder()
                .withName(PLANTED_ON)
                .withDataType(dateType)
                .build();
        Attribute ripeningTime = newAttributeBuilder()
                .withName(RIPENING_TIME)
                .withDataType(secondType)
                .build();
        Attribute producedOn = newAttributeBuilder()
                .withName(PRODUCED_ON)
                .withDataType(timestampType)
                .build();
        Attribute qualityDuration = newAttributeBuilder()
                .withName(QUALITY_DURATION)
                .withDataType(secondType)
                .build();
        EntityType fruit = newEntityTypeBuilder()
                .withName(FRUIT_ENTITY)
                .withAttributes(Arrays.asList(fruitName, plantedOn, ripeningTime, producedOn, qualityDuration))
                .build();

        final DataProperty producedIn2020 = newDataPropertyBuilder()
                .withName(PRODUCED_IN_2020)
                .withDataType(booleanType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withDialect(ExpressionDialect.JQL).withExpression("self.producedOn >= `2020-01-01T00:00:00Z` and self.producedOn < `2021-01-01T00:00:00Z`")
                        .build())
                .build();

        final DataProperty gatheredOn = newDataPropertyBuilder()
                .withName(GATHERED_ON)
                .withDataType(dateType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withDialect(ExpressionDialect.JQL).withExpression("self.plantedOn + self.ripeningTime")
                        .build())
                .build();

        final DataProperty calculatedRipeningTime = newDataPropertyBuilder()
                .withName(CALCULATED_RIPENING_TIME)
                .withDataType(dayType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withDialect(ExpressionDialect.JQL).withExpression("self.gatheredOn!elapsedTimeFrom(self.plantedOn)")
                        .build())
                .build();

        final DataProperty bestBefore = newDataPropertyBuilder()
                .withName(BEST_BEFORE)
                .withDataType(timestampType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withDialect(ExpressionDialect.JQL).withExpression("self.producedOn + self.qualityDuration")
                        .build())
                .build();

        final DataProperty calculatedQualityDuration = newDataPropertyBuilder()
                .withName(CALCULATED_QUALITY_DURATION)
                .withDataType(dayType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withDialect(ExpressionDialect.JQL).withExpression("self.bestBefore!elapsedTimeFrom(self.producedOn)")
                        .build())
                .build();

        useEntityType(fruit).withDataProperties(Arrays.asList(producedIn2020, bestBefore, gatheredOn, calculatedRipeningTime, calculatedQualityDuration)).build();

        MappedTransferObjectType fruitDTO = newMappedTransferObjectTypeBuilder()
                .withName(FRUIT_DTO)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(NAME)
                        .withDataType(stringType)
                        .withRequired(true)
                        .withBinding(fruitName)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(PLANTED_ON)
                        .withDataType(dateType)
                        .withBinding(plantedOn)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(PRODUCED_ON)
                        .withDataType(timestampType)
                        .withBinding(producedOn)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(RIPENING_TIME)
                        .withDataType(dayType)
                        .withBinding(ripeningTime)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(QUALITY_DURATION)
                        .withDataType(hourType)
                        .withBinding(qualityDuration)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(PRODUCED_IN_2020)
                        .withDataType(booleanType)
                        .withBinding(producedIn2020)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(GATHERED_ON)
                        .withDataType(dateType)
                        .withBinding(gatheredOn)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(BEST_BEFORE)
                        .withDataType(timestampType)
                        .withBinding(bestBefore)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(CALCULATED_RIPENING_TIME)
                        .withDataType(hourType)
                        .withBinding(calculatedRipeningTime)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(CALCULATED_QUALITY_DURATION)
                        .withDataType(weekType)
                        .withBinding(calculatedQualityDuration)
                        .build())
                .withEntityType(fruit)
                .build();

        Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(Arrays.asList(time, dateType, timestampType, secondType, hourType, dayType, weekType, booleanType, stringType, numericType, fruit, fruitDTO)).build();

        return model;
    }

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getPsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    void testDate(RdbmsDaoFixture daoFixture) {
        final LocalDate plantedOn = LocalDate.of(2020, 02, 12);
        final OffsetDateTime producedOn = OffsetDateTime.of(2020, 05, 10, 14, 56, 10, 123456000, ZoneOffset.UTC);

        final Double ripeningTimeInDays = 60.0;
        final Double qualityDurationInHours = 252.0;
        final Payload fruit = map(
                NAME, "strawberry",
                PLANTED_ON, plantedOn,
                RIPENING_TIME, ripeningTimeInDays,
                PRODUCED_ON, producedOn,
                QUALITY_DURATION, qualityDurationInHours);

        final EClass dto = daoFixture.getAsmUtils().getClassByFQName(MODEL_NAME + "." + FRUIT_DTO).get();

        final Payload saved = daoFixture.getDao().create(dto, fruit, null);
        final UUID id = saved.getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());

        // TODO - check daylight saving (JNG-1586)

        log.debug("Saved entity: {}", saved);
        assertEquals(plantedOn, saved.getAs(LocalDate.class, PLANTED_ON));
        assertEquals(plantedOn.plusDays(ripeningTimeInDays.longValue()), saved.getAs(LocalDate.class, GATHERED_ON));
        assertEquals(producedOn.plusHours(qualityDurationInHours.longValue()), saved.getAs(OffsetDateTime.class, BEST_BEFORE));
        assertEquals(Double.valueOf(ripeningTimeInDays * 24), saved.getAs(Double.class, CALCULATED_RIPENING_TIME));
        assertEquals(Double.valueOf(qualityDurationInHours / 168), saved.getAs(Double.class, CALCULATED_QUALITY_DURATION));
        assertEquals(true, saved.getAs(Boolean.class, PRODUCED_IN_2020));
        assertEquals(producedOn.atZoneSameInstant(ZoneOffset.UTC), saved.getAs(OffsetDateTime.class, PRODUCED_ON).atZoneSameInstant(ZoneOffset.UTC));
        daoFixture.getDao().delete(dto, id);
    }
}
