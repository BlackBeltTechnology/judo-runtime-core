package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableMap;
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
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.JudoDatasourceSingetonExtension;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.psm.data.util.builder.DataBuilders.*;
import static hu.blackbelt.judo.meta.psm.derived.util.builder.DerivedBuilders.newDataExpressionTypeBuilder;
import static hu.blackbelt.judo.meta.psm.derived.util.builder.DerivedBuilders.newDataPropertyBuilder;
import static hu.blackbelt.judo.meta.psm.measure.util.builder.MeasureBuilders.*;
import static hu.blackbelt.judo.meta.psm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.psm.service.util.builder.ServiceBuilders.*;
import static hu.blackbelt.judo.meta.psm.type.util.builder.TypeBuilders.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JudoDatasourceSingetonExtension.class)
@ExtendWith(JudoRuntimeExtension.class)
@Slf4j
public class TemporalOperationTest {
    public static final String MODEL_NAME = "temporaloperations";
    public static final String TESTER_ENTITY = "Tester";
    public static final String TESTER_DTO = "TesterDTO";

    public static final String DATE = "date";
    public static final String TIMESTAMP = "timestamp";
    public static final String TIME = "time";

    public static final String DATE_CONSTRUCTED = "dateConstructed";
    public static final String TIMESTAMP_CONSTRUCTED = "timestampConstructed";
    public static final String TIME_CONSTRUCTED = "timeConstructed";

    public static final String YEARS_OF_DATE = "yearsOfDate";
    public static final String MONTHS_OF_DATE = "monthsOfDate";
    public static final String DAYS_OF_DATE = "daysOfDate";
    public static final String YEARS_OF_TIMESTAMP = "yearsOfTimestamp";
    public static final String MONTHS_OF_TIMESTAMP = "monthsOfTimestamp";
    public static final String DAYS_OF_TIMESTAMP = "daysOfTimestamp";
    public static final String HOURS_OF_TIMESTAMP = "hoursOfTimestamp";
    public static final String MINUTES_OF_TIMESTAMP = "minutesOfTimestamp";
    public static final String SECONDS_OF_TIMESTAMP = "secondsOfTimestamp";
    public static final String MILLISECONDS_OF_TIMESTAMP = "millisecondsOfTimestamp";

    public static final String HOURS_OF_TIME = "hoursOfTime";
    public static final String MINUTES_OF_TIME = "minutesOfTime";
    public static final String SECONDS_OF_TIME = "secondsOfTime";
    public static final String MILLISECONDS_OF_TIME = "millisecondsOfTime";


    public static final String MILLISECOND_IN_MILLISECONDS = "millisecondInMilliseconds";
    public static final String SECOND_IN_MILLISECONDS = "secondInMilliseconds";
    public static final String MINUTE_IN_MILLISECONDS = "minuteInMilliseconds";
    public static final String HOUR_IN_MILLISECONDS = "hourInMilliseconds";
    public static final String DAY_IN_MILLISECONDS = "dayInMilliseconds";
    public static final String WEEK_IN_MILLISECONDS = "weekInMilliseconds";
    public static final String SECOND_IN_SECONDS = "secondInSeconds";
    public static final String MINUTE_IN_SECONDS = "minuteInSeconds";
    public static final String HOUR_IN_SECONDS = "hourInSeconds";
    public static final String DAY_IN_SECONDS = "dayInSeconds";
    public static final String WEEK_IN_SECONDS = "weekInSeconds";
    public static final String MINUTE_IN_MINUTES = "minuteInMinutes";
    public static final String HOUR_IN_MINUTES = "hourInMinutes";
    public static final String DAY_IN_MINUTES = "dayInMinutes";
    public static final String WEEK_IN_MINUTES = "weekInMinutes";
    public static final String HOUR_IN_HOURS = "hourInHours";
    public static final String DAY_IN_HOURS = "dayInHours";
    public static final String WEEK_IN_HOURS = "weekInHours";
    public static final String DAY_IN_DAYS = "dayInDays";
    public static final String WEEK_IN_DAYS = "weekInDays";
    public static final String WEEK_IN_WEEKS = "weekInWeeks";

    public static final Double DIFFERENCE = -9.0;

    public static final String STORED_DURATION_UNIT_IS_VALID_POSTFIX = "IsValid";

    public static final List<String> DURATIONS = Arrays.asList(
            MILLISECOND_IN_MILLISECONDS, SECOND_IN_MILLISECONDS, MINUTE_IN_MILLISECONDS, HOUR_IN_MILLISECONDS, DAY_IN_MILLISECONDS, WEEK_IN_MILLISECONDS,
            SECOND_IN_SECONDS, MINUTE_IN_SECONDS, HOUR_IN_SECONDS, DAY_IN_SECONDS, WEEK_IN_SECONDS,
            MINUTE_IN_MINUTES, HOUR_IN_MINUTES, DAY_IN_MINUTES, WEEK_IN_MINUTES,
            HOUR_IN_HOURS, DAY_IN_HOURS, WEEK_IN_HOURS,
            DAY_IN_DAYS, WEEK_IN_DAYS,
            WEEK_IN_WEEKS
    );

    public static final String TIMESTAMP_PLUS_MILLISECOND_IN_MILLISECONDS = "timestampPlusMsInMs";
    public static final String TIMESTAMP_PLUS_SECOND_IN_MILLISECONDS = "timestampPlusSInMs";
    public static final String TIMESTAMP_PLUS_MINUTE_IN_MILLISECONDS = "timestampPlusMInMs";
    public static final String TIMESTAMP_PLUS_HOUR_IN_MILLISECONDS = "timestampPlusHInMs";
    public static final String TIMESTAMP_PLUS_DAY_IN_MILLISECONDS = "timestampPlusDInMs";
    public static final String TIMESTAMP_PLUS_WEEK_IN_MILLISECONDS = "timestampPlusWInMs";
    public static final String TIMESTAMP_PLUS_SECOND_IN_SECONDS = "timestampPlusSInS";
    public static final String TIMESTAMP_PLUS_MINUTE_IN_SECONDS = "timestampPlusMInS";
    public static final String TIMESTAMP_PLUS_HOUR_IN_SECONDS = "timestampPlusHInS";
    public static final String TIMESTAMP_PLUS_DAY_IN_SECONDS = "timestampPlusDInS";
    public static final String TIMESTAMP_PLUS_WEEK_IN_SECONDS = "timestampPlusWInS";
    public static final String TIMESTAMP_PLUS_MINUTE_IN_MINUTES = "timestampPlusMInM";
    public static final String TIMESTAMP_PLUS_HOUR_IN_MINUTES = "timestampPlusHInM";
    public static final String TIMESTAMP_PLUS_DAY_IN_MINUTES = "timestampPlusDInM";
    public static final String TIMESTAMP_PLUS_WEEK_IN_MINUTES = "timestampPlusWInM";
    public static final String TIMESTAMP_PLUS_HOUR_IN_HOURS = "timestampPlusHInH";
    public static final String TIMESTAMP_PLUS_DAY_IN_HOURS = "timestampPlusDInH";
    public static final String TIMESTAMP_PLUS_WEEK_IN_HOURS = "timestampPlusWInH";
    public static final String TIMESTAMP_PLUS_DAY_IN_DAYS = "timestampPlusDInD";
    public static final String TIMESTAMP_PLUS_WEEK_IN_DAYS = "timestampPlusWInD";
    public static final String TIMESTAMP_PLUS_WEEK_IN_WEEKS = "timestampPlusWInW";

    public static final String TIME_PLUS_MILLISECOND_IN_MILLISECONDS = "timePlusMsInMs";
    public static final String TIME_PLUS_SECOND_IN_MILLISECONDS = "timePlusSInMs";
    public static final String TIME_PLUS_MINUTE_IN_MILLISECONDS = "timePlusMInMs";
    public static final String TIME_PLUS_HOUR_IN_MILLISECONDS = "timePlusHInMs";
    public static final String TIME_PLUS_SECOND_IN_SECONDS = "timePlusSInS";
    public static final String TIME_PLUS_MINUTE_IN_SECONDS = "timePlusMInS";
    public static final String TIME_PLUS_HOUR_IN_SECONDS = "timePlusHInS";
    public static final String TIME_PLUS_MINUTE_IN_MINUTES = "timePlusMInM";
    public static final String TIME_PLUS_HOUR_IN_MINUTES = "timePlusHInM";
    public static final String TIME_PLUS_HOUR_IN_HOURS = "timePlusHInH";


    public static final Map<String, ChronoUnit> TIMESTAMP_ADDITIONS = ImmutableMap.<String, ChronoUnit>builder()
            .put(TIMESTAMP_PLUS_MILLISECOND_IN_MILLISECONDS, ChronoUnit.MILLIS)
            .put(TIMESTAMP_PLUS_SECOND_IN_MILLISECONDS, ChronoUnit.SECONDS)
            .put(TIMESTAMP_PLUS_SECOND_IN_SECONDS, ChronoUnit.SECONDS)
            .put(TIMESTAMP_PLUS_MINUTE_IN_MILLISECONDS, ChronoUnit.MINUTES)
            .put(TIMESTAMP_PLUS_MINUTE_IN_SECONDS, ChronoUnit.MINUTES)
            .put(TIMESTAMP_PLUS_MINUTE_IN_MINUTES, ChronoUnit.MINUTES)
            .put(TIMESTAMP_PLUS_HOUR_IN_MILLISECONDS, ChronoUnit.HOURS)
            .put(TIMESTAMP_PLUS_HOUR_IN_SECONDS, ChronoUnit.HOURS)
            .put(TIMESTAMP_PLUS_HOUR_IN_MINUTES, ChronoUnit.HOURS)
            .put(TIMESTAMP_PLUS_HOUR_IN_HOURS, ChronoUnit.HOURS)
            .put(TIMESTAMP_PLUS_DAY_IN_MILLISECONDS, ChronoUnit.DAYS)
            .put(TIMESTAMP_PLUS_DAY_IN_SECONDS, ChronoUnit.DAYS)
            .put(TIMESTAMP_PLUS_DAY_IN_MINUTES, ChronoUnit.DAYS)
            .put(TIMESTAMP_PLUS_DAY_IN_HOURS, ChronoUnit.DAYS)
            .put(TIMESTAMP_PLUS_DAY_IN_DAYS, ChronoUnit.DAYS)
            .put(TIMESTAMP_PLUS_WEEK_IN_MILLISECONDS, ChronoUnit.WEEKS)
            .put(TIMESTAMP_PLUS_WEEK_IN_SECONDS, ChronoUnit.WEEKS)
            .put(TIMESTAMP_PLUS_WEEK_IN_MINUTES, ChronoUnit.WEEKS)
            .put(TIMESTAMP_PLUS_WEEK_IN_HOURS, ChronoUnit.WEEKS)
            .put(TIMESTAMP_PLUS_WEEK_IN_DAYS, ChronoUnit.WEEKS)
            .put(TIMESTAMP_PLUS_WEEK_IN_WEEKS, ChronoUnit.WEEKS)
            .build();

    public static final Map<String, ChronoUnit> TIME_ADDITIONS = ImmutableMap.<String, ChronoUnit>builder()
            .put(TIME_PLUS_MILLISECOND_IN_MILLISECONDS, ChronoUnit.MILLIS)
            .put(TIME_PLUS_SECOND_IN_MILLISECONDS, ChronoUnit.SECONDS)
            .put(TIME_PLUS_SECOND_IN_SECONDS, ChronoUnit.SECONDS)
            .put(TIME_PLUS_MINUTE_IN_MILLISECONDS, ChronoUnit.MINUTES)
            .put(TIME_PLUS_MINUTE_IN_SECONDS, ChronoUnit.MINUTES)
            .put(TIME_PLUS_MINUTE_IN_MINUTES, ChronoUnit.MINUTES)
            .put(TIME_PLUS_HOUR_IN_MILLISECONDS, ChronoUnit.HOURS)
            .put(TIME_PLUS_HOUR_IN_SECONDS, ChronoUnit.HOURS)
            .put(TIME_PLUS_HOUR_IN_MINUTES, ChronoUnit.HOURS)
            .put(TIME_PLUS_HOUR_IN_HOURS, ChronoUnit.HOURS)
            .build();


    public static final String DATE_PLUS_DAY_IN_MILLISECONDS = "datePlusDInMs";
    public static final String DATE_PLUS_WEEK_IN_MILLISECONDS = "datePlusWInMs";
    public static final String DATE_PLUS_DAY_IN_SECONDS = "datePlusDInS";
    public static final String DATE_PLUS_WEEK_IN_SECONDS = "datePlusWInS";
    public static final String DATE_PLUS_DAY_IN_MINUTES = "datePlusDInM";
    public static final String DATE_PLUS_WEEK_IN_MINUTES = "datePlusWInM";
    public static final String DATE_PLUS_DAY_IN_HOURS = "datePlusDInH";
    public static final String DATE_PLUS_WEEK_IN_HOURS = "datePlusWInH";
    public static final String DATE_PLUS_DAY_IN_DAYS = "datePlusDInD";
    public static final String DATE_PLUS_WEEK_IN_DAYS = "datePlusWInD";
    public static final String DATE_PLUS_WEEK_IN_WEEKS = "datePlusWInW";

    public static final Map<String, ChronoUnit> DATE_ADDITIONS = ImmutableMap.<String, ChronoUnit>builder()
            .put(DATE_PLUS_DAY_IN_MILLISECONDS, ChronoUnit.DAYS)
            .put(DATE_PLUS_DAY_IN_SECONDS, ChronoUnit.DAYS)
            .put(DATE_PLUS_DAY_IN_MINUTES, ChronoUnit.DAYS)
            .put(DATE_PLUS_DAY_IN_HOURS, ChronoUnit.DAYS)
            .put(DATE_PLUS_DAY_IN_DAYS, ChronoUnit.DAYS)
            .put(DATE_PLUS_WEEK_IN_MILLISECONDS, ChronoUnit.WEEKS)
            .put(DATE_PLUS_WEEK_IN_SECONDS, ChronoUnit.WEEKS)
            .put(DATE_PLUS_WEEK_IN_MINUTES, ChronoUnit.WEEKS)
            .put(DATE_PLUS_WEEK_IN_HOURS, ChronoUnit.WEEKS)
            .put(DATE_PLUS_WEEK_IN_DAYS, ChronoUnit.WEEKS)
            .put(DATE_PLUS_WEEK_IN_WEEKS, ChronoUnit.WEEKS)
            .build();

    public static final String CALCULATED_DURATION_UNIT = "Duration";

    protected String getModelName() {
        return MODEL_NAME;
    }

    @Getter
    @Builder
    static class AttributeDef {

        private String name;

        private Primitive entityAttributeType;

        private Primitive transferAttributeType;
    }

    @Getter
    @Builder
    static class PropertyDef {

        private String name;

        private Primitive entityAttributeType;

        private String expression;

        private Primitive transferAttributeType;
    }

    private void addAttributes(final EntityType entityType, final MappedTransferObjectType transferObjectType, final BooleanType booleanType, final Collection<AttributeDef> attributes) {
        final Map<String, Attribute> createdAttributes = attributes.stream()
                .collect(Collectors.toMap(
                        e -> e.getName(),
                        e -> newAttributeBuilder()
                                .withName(e.getName())
                                .withDataType(e.getEntityAttributeType())
                                .build()));

        useEntityType(entityType).withAttributes(createdAttributes.values()).build();

        attributes.forEach(attribute ->
                useMappedTransferObjectType(transferObjectType)
                        .withAttributes(newTransferAttributeBuilder()
                                .withName(attribute.getName())
                                .withDataType(attribute.getTransferAttributeType())
                                .withBinding(createdAttributes.get(attribute.getName()))
                                .build())
                        .build());

        final Map<String, DataProperty> createdProperties = attributes.stream()
                .filter(e -> e.getTransferAttributeType() instanceof MeasuredType)
                .collect(Collectors.toMap(
                        e -> e.getName(),
                        e -> newDataPropertyBuilder()
                                .withName(e.getName() + STORED_DURATION_UNIT_IS_VALID_POSTFIX)
                                .withDataType(booleanType)
                                .withGetterExpression(newDataExpressionTypeBuilder()
                                        .withDialect(ExpressionDialect.JQL).withExpression("self." + e.getName() + " == " + DIFFERENCE + "[" + ((MeasuredType) e.getTransferAttributeType()).getStoreUnit().getSymbol() + "]")
                                        .build())
                                .build()));

        useEntityType(entityType).withDataProperties(createdProperties.values()).build();

        attributes.forEach(attribute ->
                useMappedTransferObjectType(transferObjectType)
                        .withAttributes(newTransferAttributeBuilder()
                                .withName(attribute.getName() + STORED_DURATION_UNIT_IS_VALID_POSTFIX)
                                .withDataType(booleanType)
                                .withBinding(createdProperties.get(attribute.getName()))
                                .build())
                        .build());
    }

    private void addProperties(final EntityType entityType, final MappedTransferObjectType transferObjectType, final Primitive type, final String baseName, final Collection<PropertyDef> properties) {
        final Map<String, DataProperty> createdProperties = properties.stream()
                .collect(Collectors.toMap(
                        e -> e.getName(),
                        e -> newDataPropertyBuilder()
                                .withName(e.getName())
                                .withDataType(type)
                                .withGetterExpression(newDataExpressionTypeBuilder()
                                        .withDialect(ExpressionDialect.JQL).withExpression(e.getExpression())
                                        .build())
                                .build()));

        useEntityType(entityType).withDataProperties(createdProperties.values()).build();

        final Map<String, DataProperty> createdPropertiesCalculated = properties.stream()
                .collect(Collectors.toMap(
                        e -> e.getName(),
                        e -> newDataPropertyBuilder()
                                .withName(e.getName() + CALCULATED_DURATION_UNIT)
                                .withDataType(e.getEntityAttributeType())
                                .withGetterExpression(newDataExpressionTypeBuilder()
                                        .withDialect(ExpressionDialect.JQL).withExpression("self." + e.getName() + "!elapsedTimeFrom(self." + baseName + ")")
                                        .build())
                                .build()));

        useEntityType(entityType).withDataProperties(createdPropertiesCalculated.values()).build();

        properties.forEach(attribute ->
                useMappedTransferObjectType(transferObjectType)
                        .withAttributes(newTransferAttributeBuilder()
                                .withName(attribute.getName())
                                .withDataType(type)
                                .withBinding(createdProperties.get(attribute.getName()))
                                .build())
                        .build());

        properties.forEach(attribute ->
                useMappedTransferObjectType(transferObjectType)
                        .withAttributes(newTransferAttributeBuilder()
                                .withName(attribute.getName() + CALCULATED_DURATION_UNIT)
                                .withDataType(attribute.getTransferAttributeType())
                                .withBinding(createdPropertiesCalculated.get(attribute.getName()))
                                .build())
                        .build());
    }

    protected Model getPsmModel() {
        final Measure timeMeasure = newMeasureBuilder().withName("Time").withSymbol("t").build();
        final DurationUnit nanosecond = newDurationUnitBuilder().withName("nanosecond").withSymbol("ns").withRateDividend(0.001).withRateDivisor(1000000).withUnitType(DurationType.NANOSECOND).build();
        final DurationUnit microsecond = newDurationUnitBuilder().withName("microsecond").withSymbol("μs").withRateDividend(1).withRateDivisor(1000000).withUnitType(DurationType.MICROSECOND).build();
        final DurationUnit millisecond = newDurationUnitBuilder().withName("millisecond").withSymbol("ms").withRateDividend(1).withRateDivisor(1000).withUnitType(DurationType.MILLISECOND).build();
        final DurationUnit second = newDurationUnitBuilder().withName("second").withSymbol("s").withRateDividend(1).withRateDivisor(1).withUnitType(DurationType.SECOND).build();
        final DurationUnit minute = newDurationUnitBuilder().withName("minute").withSymbol("m").withRateDividend(60).withRateDivisor(1).withUnitType(DurationType.MINUTE).build();
        final DurationUnit hour = newDurationUnitBuilder().withName("hour").withSymbol("h").withRateDividend(3600).withRateDivisor(1).withUnitType(DurationType.HOUR).build();
        final DurationUnit day = newDurationUnitBuilder().withName("day").withSymbol("d").withRateDividend(86400).withRateDivisor(1).withUnitType(DurationType.DAY).build();
        final DurationUnit week = newDurationUnitBuilder().withName("week").withSymbol("w").withRateDividend(604800).withRateDivisor(1).withUnitType(DurationType.WEEK).build();
        useMeasure(timeMeasure).withUnits(Arrays.asList(nanosecond, microsecond, millisecond, second, minute, hour, day, week)).build();

        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean").build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer").withPrecision(9).withScale(0).build();
        final DateType dateType = newDateTypeBuilder().withName("Date").build();
        final TimestampType timestampType = newTimestampTypeBuilder().withName("Timestamp").withBaseUnit(DurationType.MICROSECOND).build();
        final TimeType timeType = newTimeTypeBuilder().withName("Time").withBaseUnit(DurationType.MICROSECOND).build();
        final MeasuredType nanosecondType = newMeasuredTypeBuilder().withName("Nanosecond").withPrecision(15).withScale(4).withStoreUnit(nanosecond).build();
        final MeasuredType microsecondType = newMeasuredTypeBuilder().withName("Microsecond").withPrecision(15).withScale(4).withStoreUnit(microsecond).build();
        final MeasuredType millisecondType = newMeasuredTypeBuilder().withName("Millisecond").withPrecision(15).withScale(4).withStoreUnit(millisecond).build();
        final MeasuredType secondType = newMeasuredTypeBuilder().withName("Second").withPrecision(15).withScale(4).withStoreUnit(second).build();
        final MeasuredType minuteType = newMeasuredTypeBuilder().withName("Minute").withPrecision(15).withScale(4).withStoreUnit(minute).build();
        final MeasuredType hourType = newMeasuredTypeBuilder().withName("Hour").withPrecision(15).withScale(4).withStoreUnit(hour).build();
        final MeasuredType dayType = newMeasuredTypeBuilder().withName("Day").withPrecision(15).withScale(4).withStoreUnit(day).build();
        final MeasuredType weekType = newMeasuredTypeBuilder().withName("Week").withPrecision(15).withScale(4).withStoreUnit(week).build();

        Attribute date = newAttributeBuilder()
                .withName(DATE)
                .withDataType(dateType)
                .build();
        Attribute timestamp = newAttributeBuilder()
                .withName(TIMESTAMP)
                .withDataType(timestampType)
                .build();
        Attribute time = newAttributeBuilder()
                .withName(TIME)
                .withDataType(timeType)
                .build();

        DataProperty yearsOfDate = newDataPropertyBuilder()
                .withName(YEARS_OF_DATE)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + DATE + "!year()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty monthOfDate = newDataPropertyBuilder()
                .withName(MONTHS_OF_DATE)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + DATE + "!month()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty daysOfDate = newDataPropertyBuilder()
                .withName(DAYS_OF_DATE)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + DATE + "!day()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty yearsOfTimestamp = newDataPropertyBuilder()
                .withName(YEARS_OF_TIMESTAMP)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIMESTAMP + "!year()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty monthOfTimestamp = newDataPropertyBuilder()
                .withName(MONTHS_OF_TIMESTAMP)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIMESTAMP + "!month()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty daysOfTimestamp = newDataPropertyBuilder()
                .withName(DAYS_OF_TIMESTAMP)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIMESTAMP + "!day()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty hoursOfTimestamp = newDataPropertyBuilder()
                .withName(HOURS_OF_TIMESTAMP)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIMESTAMP + "!hour()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty minutesOfTimestamp = newDataPropertyBuilder()
                .withName(MINUTES_OF_TIMESTAMP)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIMESTAMP + "!minute()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty secondsOfTimestamp = newDataPropertyBuilder()
                .withName(SECONDS_OF_TIMESTAMP)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIMESTAMP + "!second()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty millisecondsOfTimestamp = newDataPropertyBuilder()
                .withName(MILLISECONDS_OF_TIMESTAMP)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIMESTAMP + "!millisecond()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();

        DataProperty hoursOfTime = newDataPropertyBuilder()
                .withName(HOURS_OF_TIME)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIME + "!hour()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty minutesOfTime = newDataPropertyBuilder()
                .withName(MINUTES_OF_TIME)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIME + "!minute()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty secondsOfTime = newDataPropertyBuilder()
                .withName(SECONDS_OF_TIME)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIME + "!second()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty millisecondsOfTime = newDataPropertyBuilder()
                .withName(MILLISECONDS_OF_TIME)
                .withDataType(integerType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression("self." + TIME + "!millisecond()")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();

        DataProperty dateConstructed = newDataPropertyBuilder()
                .withName(DATE_CONSTRUCTED)
                .withDataType(dateType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression(MODEL_NAME + "::Date!of(self." + YEARS_OF_DATE + ", self." + MONTHS_OF_DATE + ", self." + DAYS_OF_DATE + ")")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty timestampConstructed = newDataPropertyBuilder()
                .withName(TIMESTAMP_CONSTRUCTED)
                .withDataType(timestampType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression(MODEL_NAME + "::Timestamp!of(self." + YEARS_OF_TIMESTAMP + ", self." + MONTHS_OF_TIMESTAMP + ", self." + DAYS_OF_TIMESTAMP + ", self." + HOURS_OF_TIMESTAMP + ", self." + MINUTES_OF_TIMESTAMP + ", self." + SECONDS_OF_TIMESTAMP + ")")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();
        DataProperty timeConstructed = newDataPropertyBuilder()
                .withName(TIME_CONSTRUCTED)
                .withDataType(timeType)
                .withGetterExpression(newDataExpressionTypeBuilder()
                        .withExpression(MODEL_NAME + "::Time!of(self." + HOURS_OF_TIME + ", self." + MINUTES_OF_TIME + ", self." + SECONDS_OF_TIME + ")")
                        .withDialect(ExpressionDialect.JQL)
                        .build())
                .build();

        EntityType tester = newEntityTypeBuilder()
                .withName(TESTER_ENTITY)
                .withAttributes(date)
                .withAttributes(timestamp)
                .withAttributes(time)
                .withDataProperties(yearsOfDate, monthOfDate, daysOfDate)
                .withDataProperties(yearsOfTimestamp, monthOfTimestamp, daysOfTimestamp, hoursOfTimestamp, minutesOfTimestamp, secondsOfTimestamp, millisecondsOfTimestamp)
                .withDataProperties(hoursOfTime, minutesOfTime, secondsOfTime, millisecondsOfTime)
                .withDataProperties(dateConstructed, timestampConstructed, timeConstructed)
                .build();

        MappedTransferObjectType testerDTO = newMappedTransferObjectTypeBuilder()
                .withName(TESTER_DTO)
                .withAttributes(newTransferAttributeBuilder()
                        .withName(DATE)
                        .withDataType(dateType)
                        .withBinding(date)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(TIMESTAMP)
                        .withDataType(timestampType)
                        .withBinding(timestamp)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(TIME)
                        .withDataType(timeType)
                        .withBinding(time)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(YEARS_OF_DATE)
                        .withDataType(integerType)
                        .withBinding(yearsOfDate)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(MONTHS_OF_DATE)
                        .withDataType(integerType)
                        .withBinding(monthOfDate)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(DAYS_OF_DATE)
                        .withDataType(integerType)
                        .withBinding(daysOfDate)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(YEARS_OF_TIMESTAMP)
                        .withDataType(integerType)
                        .withBinding(yearsOfTimestamp)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(MONTHS_OF_TIMESTAMP)
                        .withDataType(integerType)
                        .withBinding(monthOfTimestamp)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(DAYS_OF_TIMESTAMP)
                        .withDataType(integerType)
                        .withBinding(daysOfTimestamp)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(HOURS_OF_TIMESTAMP)
                        .withDataType(integerType)
                        .withBinding(hoursOfTimestamp)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(MINUTES_OF_TIMESTAMP)
                        .withDataType(integerType)
                        .withBinding(minutesOfTimestamp)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(SECONDS_OF_TIMESTAMP)
                        .withDataType(integerType)
                        .withBinding(secondsOfTimestamp)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(MILLISECONDS_OF_TIMESTAMP)
                        .withDataType(integerType)
                        .withBinding(millisecondsOfTimestamp)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(HOURS_OF_TIME)
                        .withDataType(integerType)
                        .withBinding(hoursOfTime)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(MINUTES_OF_TIME)
                        .withDataType(integerType)
                        .withBinding(minutesOfTime)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(SECONDS_OF_TIME)
                        .withDataType(integerType)
                        .withBinding(secondsOfTime)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(MILLISECONDS_OF_TIME)
                        .withDataType(integerType)
                        .withBinding(millisecondsOfTime)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(DATE_CONSTRUCTED)
                        .withDataType(dateType)
                        .withBinding(dateConstructed)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(TIMESTAMP_CONSTRUCTED)
                        .withDataType(timestampType)
                        .withBinding(timestampConstructed)
                        .build())
                .withAttributes(newTransferAttributeBuilder()
                        .withName(TIME_CONSTRUCTED)
                        .withDataType(timeType)
                        .withBinding(timeConstructed)
                        .build())
                .withEntityType(tester)
                .build();

        addAttributes(tester, testerDTO, booleanType, Arrays.asList(
                AttributeDef.builder().name(MILLISECOND_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(millisecondType).build(),
                AttributeDef.builder().name(SECOND_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(secondType).build(),
                AttributeDef.builder().name(MINUTE_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(minuteType).build(),
                AttributeDef.builder().name(HOUR_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(hourType).build(),
                AttributeDef.builder().name(DAY_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(dayType).build(),
                AttributeDef.builder().name(WEEK_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(weekType).build(),
                AttributeDef.builder().name(SECOND_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(secondType).build(),
                AttributeDef.builder().name(MINUTE_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(minuteType).build(),
                AttributeDef.builder().name(HOUR_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(hourType).build(),
                AttributeDef.builder().name(DAY_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(dayType).build(),
                AttributeDef.builder().name(WEEK_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(weekType).build(),
                AttributeDef.builder().name(MINUTE_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(minuteType).build(),
                AttributeDef.builder().name(HOUR_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(hourType).build(),
                AttributeDef.builder().name(DAY_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(dayType).build(),
                AttributeDef.builder().name(WEEK_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(weekType).build(),
                AttributeDef.builder().name(HOUR_IN_HOURS).entityAttributeType(hourType).transferAttributeType(hourType).build(),
                AttributeDef.builder().name(DAY_IN_HOURS).entityAttributeType(hourType).transferAttributeType(dayType).build(),
                AttributeDef.builder().name(WEEK_IN_HOURS).entityAttributeType(hourType).transferAttributeType(weekType).build(),
                AttributeDef.builder().name(DAY_IN_DAYS).entityAttributeType(dayType).transferAttributeType(dayType).build(),
                AttributeDef.builder().name(WEEK_IN_DAYS).entityAttributeType(dayType).transferAttributeType(weekType).build(),
                AttributeDef.builder().name(WEEK_IN_WEEKS).entityAttributeType(weekType).transferAttributeType(weekType).build()
        ));
        addProperties(tester, testerDTO, timestampType, TIMESTAMP, Arrays.asList(
                PropertyDef.builder().name(TIMESTAMP_PLUS_MILLISECOND_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(millisecondType).expression("self." + TIMESTAMP + " + 2 * self." + MILLISECOND_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_SECOND_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(secondType).expression("self." + TIMESTAMP + " + 2 * self." + SECOND_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_MINUTE_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(minuteType).expression("self." + TIMESTAMP + " + 2 * self." + MINUTE_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_HOUR_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(hourType).expression("self." + TIMESTAMP + " + 2 * self." + HOUR_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_DAY_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(dayType).expression("self." + TIMESTAMP + " + 2 * self." + DAY_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_WEEK_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(weekType).expression("self." + TIMESTAMP + " + 2 * self." + WEEK_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_SECOND_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(secondType).expression("self." + TIMESTAMP + " + 2 * self." + SECOND_IN_SECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_MINUTE_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(minuteType).expression("self." + TIMESTAMP + " + 2 * self." + MINUTE_IN_SECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_HOUR_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(hourType).expression("self." + TIMESTAMP + " + 2 * self." + HOUR_IN_SECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_DAY_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(dayType).expression("self." + TIMESTAMP + " + 2 * self." + DAY_IN_SECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_WEEK_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(weekType).expression("self." + TIMESTAMP + " + 2 * self." + WEEK_IN_SECONDS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_MINUTE_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(minuteType).expression("self." + TIMESTAMP + " + 2 * self." + MINUTE_IN_MINUTES).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_HOUR_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(hourType).expression("self." + TIMESTAMP + " + 2 * self." + HOUR_IN_MINUTES).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_DAY_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(dayType).expression("self." + TIMESTAMP + " + 2 * self." + DAY_IN_MINUTES).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_WEEK_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(weekType).expression("self." + TIMESTAMP + " + 2 * self." + WEEK_IN_MINUTES).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_HOUR_IN_HOURS).entityAttributeType(hourType).transferAttributeType(hourType).expression("self." + TIMESTAMP + " + 2 * self." + HOUR_IN_HOURS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_DAY_IN_HOURS).entityAttributeType(hourType).transferAttributeType(dayType).expression("self." + TIMESTAMP + " + 2 * self." + DAY_IN_HOURS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_WEEK_IN_HOURS).entityAttributeType(hourType).transferAttributeType(weekType).expression("self." + TIMESTAMP + " + 2 * self." + WEEK_IN_HOURS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_DAY_IN_DAYS).entityAttributeType(dayType).transferAttributeType(dayType).expression("self." + TIMESTAMP + " + 2 * self." + DAY_IN_DAYS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_WEEK_IN_DAYS).entityAttributeType(dayType).transferAttributeType(weekType).expression("self." + TIMESTAMP + " + 2 * self." + WEEK_IN_DAYS).build(),
                PropertyDef.builder().name(TIMESTAMP_PLUS_WEEK_IN_WEEKS).entityAttributeType(weekType).transferAttributeType(weekType).expression("self." + TIMESTAMP + " + 2 * self." + WEEK_IN_WEEKS).build()
        ));
        addProperties(tester, testerDTO, timeType, TIME, Arrays.asList(
                PropertyDef.builder().name(TIME_PLUS_MILLISECOND_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(millisecondType).expression("self." + TIME + " + 2 * self." + MILLISECOND_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(TIME_PLUS_SECOND_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(secondType).expression("self." + TIME + " + 2 * self." + SECOND_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(TIME_PLUS_MINUTE_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(minuteType).expression("self." + TIME + " + 2 * self." + MINUTE_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(TIME_PLUS_HOUR_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(hourType).expression("self." + TIME + " + 2 * self." + HOUR_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(TIME_PLUS_SECOND_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(secondType).expression("self." + TIME + " + 2 * self." + SECOND_IN_SECONDS).build(),
                PropertyDef.builder().name(TIME_PLUS_MINUTE_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(minuteType).expression("self." + TIME + " + 2 * self." + MINUTE_IN_SECONDS).build(),
                PropertyDef.builder().name(TIME_PLUS_HOUR_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(hourType).expression("self." + TIME + " + 2 * self." + HOUR_IN_SECONDS).build(),
                PropertyDef.builder().name(TIME_PLUS_MINUTE_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(minuteType).expression("self." + TIME + " + 2 * self." + MINUTE_IN_MINUTES).build(),
                PropertyDef.builder().name(TIME_PLUS_HOUR_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(hourType).expression("self." + TIME + " + 2 * self." + HOUR_IN_MINUTES).build(),
                PropertyDef.builder().name(TIME_PLUS_HOUR_IN_HOURS).entityAttributeType(hourType).transferAttributeType(hourType).expression("self." + TIME + " + 2 * self." + HOUR_IN_HOURS).build()
        ));
        addProperties(tester, testerDTO, dateType, DATE, Arrays.asList(
                PropertyDef.builder().name(DATE_PLUS_DAY_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(dayType).expression("self." + DATE + " + 2 * self." + DAY_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(DATE_PLUS_WEEK_IN_MILLISECONDS).entityAttributeType(millisecondType).transferAttributeType(weekType).expression("self." + DATE + " + 2 * self." + WEEK_IN_MILLISECONDS).build(),
                PropertyDef.builder().name(DATE_PLUS_DAY_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(dayType).expression("self." + DATE + " + 2 * self." + DAY_IN_SECONDS).build(),
                PropertyDef.builder().name(DATE_PLUS_WEEK_IN_SECONDS).entityAttributeType(secondType).transferAttributeType(weekType).expression("self." + DATE + " + 2 * self." + WEEK_IN_SECONDS).build(),
                PropertyDef.builder().name(DATE_PLUS_DAY_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(dayType).expression("self." + DATE + " + 2 * self." + DAY_IN_MINUTES).build(),
                PropertyDef.builder().name(DATE_PLUS_WEEK_IN_MINUTES).entityAttributeType(minuteType).transferAttributeType(weekType).expression("self." + DATE + " + 2 * self." + WEEK_IN_MINUTES).build(),
                PropertyDef.builder().name(DATE_PLUS_DAY_IN_HOURS).entityAttributeType(hourType).transferAttributeType(dayType).expression("self." + DATE + " + 2 * self." + DAY_IN_HOURS).build(),
                PropertyDef.builder().name(DATE_PLUS_WEEK_IN_HOURS).entityAttributeType(hourType).transferAttributeType(weekType).expression("self." + DATE + " + 2 * self." + WEEK_IN_HOURS).build(),
                PropertyDef.builder().name(DATE_PLUS_DAY_IN_DAYS).entityAttributeType(dayType).transferAttributeType(dayType).expression("self." + DATE + " + 2 * self." + DAY_IN_DAYS).build(),
                PropertyDef.builder().name(DATE_PLUS_WEEK_IN_DAYS).entityAttributeType(dayType).transferAttributeType(weekType).expression("self." + DATE + " + 2 * self." + WEEK_IN_DAYS).build(),
                PropertyDef.builder().name(DATE_PLUS_WEEK_IN_WEEKS).entityAttributeType(weekType).transferAttributeType(weekType).expression("self." + DATE + " + 2 * self." + WEEK_IN_WEEKS).build()
        ));
        log.debug("Data properties:{}", tester.getAllDataProperties().stream().map(p -> "\n  - " + p.getName() + " -> " + p.getGetterExpression().getExpression()).collect(Collectors.joining()));

        Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(timeMeasure, dateType, timestampType, timeType, nanosecondType, microsecondType, millisecondType, secondType, minuteType, hourType, dayType, weekType,
                        booleanType, integerType, tester, testerDTO).build();

        return model;
    }

    @BeforeEach
    public void setup(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.init(getPsmModel(), datasourceFixture);
        assertTrue(runtimeFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
        runtimeFixture.dropDatabase();
    }

    @Test
    void testOperations(JudoRuntimeFixture testFixture, JudoDatasourceFixture datasourceFixture) {
        final LocalDate date = LocalDate.of(2019, 02, 03);
        final OffsetDateTime timestamp = LocalDateTime.of(2019, 03, 04, 02, 05, 06, 0)
                .atOffset(ZoneOffset.systemDefault().getRules().getOffset(ZonedDateTime.now().toInstant()));
        final LocalTime time = LocalTime.of(02, 05, 06, 0);
        final Payload payload = map(
                DATE, date,
                TIMESTAMP, timestamp,
                TIME, time);

        payload.putAll(DURATIONS.stream().collect(Collectors.toMap(e -> e, e -> DIFFERENCE)));

        final EClass dto = testFixture.getAsmUtils().getClassByFQName(MODEL_NAME + "." + TESTER_DTO).get();

        final Payload saved = testFixture.getDao().create(dto, payload, null);
        final UUID id = saved.getAs(testFixture.getIdProvider().getType(), testFixture.getIdProvider().getName());

        log.debug("Saved entity: {}", saved.entrySet().stream().map(e -> "\n  - " + e.getKey() + ": " + e.getValue()).collect(Collectors.joining()));
        assertEquals(date, saved.getAs(LocalDate.class, DATE));
        assertEquals(timestamp.atZoneSameInstant(ZoneOffset.UTC),
                saved.getAs(OffsetDateTime.class, TIMESTAMP).atZoneSameInstant(ZoneOffset.UTC));
        assertEquals(time, saved.getAs(LocalTime.class, TIME));
        assertEquals(2019, saved.getAs(Integer.class, YEARS_OF_DATE));
        assertEquals(2, saved.getAs(Integer.class, MONTHS_OF_DATE));
        assertEquals(3, saved.getAs(Integer.class, DAYS_OF_DATE));
        assertEquals(2019, saved.getAs(Integer.class, YEARS_OF_TIMESTAMP));
        assertEquals(3, saved.getAs(Integer.class, MONTHS_OF_TIMESTAMP));
        assertEquals(4, saved.getAs(Integer.class, DAYS_OF_TIMESTAMP));
        // FIXME: https://blackbelt.atlassian.net/browse/JNG-3681 Time offset
//        assertEquals(timestamp.atZoneSameInstant(ZoneOffset.UTC).getHour(),
//             saved.getAs(Integer.class, HOURS_OF_TIMESTAMP));
        assertEquals(5, saved.getAs(Integer.class, MINUTES_OF_TIMESTAMP));
        assertEquals(6, saved.getAs(Integer.class, SECONDS_OF_TIMESTAMP));
//        assertEquals(123, saved.getAs(Integer.class, MILLISECONDS_OF_TIMESTAMP));

        assertEquals(2, saved.getAs(Integer.class, HOURS_OF_TIME));
        assertEquals(5, saved.getAs(Integer.class, MINUTES_OF_TIME));
        assertEquals(6, saved.getAs(Integer.class, SECONDS_OF_TIME));

        assertEquals(date, saved.getAs(LocalDate.class, DATE_CONSTRUCTED));
        assertEquals(time, saved.getAs(LocalTime.class, TIME_CONSTRUCTED));

        // FIXME: https://blackbelt.atlassian.net/browse/JNG-3681 Expected: 2019-03-04T00:05:06, Actual: 2019-03-04T01:05:06
//        assertEquals(timestamp.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime(),
//                saved.getAs(OffsetDateTime.class, TIMESTAMP_CONSTRUCTED).toLocalDateTime());

        DURATIONS.forEach(duration -> {
            log.debug("Checking saved duration {}...", duration);
            if (!MILLISECOND_IN_MILLISECONDS.equals(duration)) {
                assertEquals(DIFFERENCE, saved.getAs(Double.class, duration));
                assertEquals(Boolean.TRUE, saved.getAs(Boolean.class, duration + STORED_DURATION_UNIT_IS_VALID_POSTFIX));
            }
        });

        TIMESTAMP_ADDITIONS.entrySet().stream()
                .filter(e -> !ChronoUnit.MILLIS.equals(e.getValue())) // millisecond assertions are skipped because it is not supported yet
                .forEach(e -> {
                    log.debug("Checking saved timestamp addition {}...", e.getKey());
                    assertEquals(timestamp.plus(2 * DIFFERENCE.longValue(), e.getValue()).atZoneSameInstant(ZoneOffset.UTC), saved.getAs(OffsetDateTime.class, e.getKey()).atZoneSameInstant(ZoneOffset.UTC));
                    log.debug("Checking saved timestamp difference {}...", e.getKey() + CALCULATED_DURATION_UNIT);
                    assertEquals(Double.valueOf(2 * DIFFERENCE), saved.getAs(Double.class, e.getKey() + CALCULATED_DURATION_UNIT));
                });

        TIME_ADDITIONS.entrySet().stream()
                .filter(e -> !ChronoUnit.MILLIS.equals(e.getValue())) // millisecond assertions are skipped because it is not supported yet
                .forEach(e -> {
                    log.debug("Checking saved time addition {}...", e.getKey());
                    assertEquals(time.plus(2 * DIFFERENCE.longValue(), e.getValue()), saved.getAs(LocalTime.class, e.getKey()));
                    log.debug("Checking saved time difference {}...", e.getKey() + CALCULATED_DURATION_UNIT);
                    assertEquals(Double.valueOf(2 * DIFFERENCE), saved.getAs(Double.class, e.getKey() + CALCULATED_DURATION_UNIT));
                });

        DATE_ADDITIONS.entrySet().stream()
                .filter(e -> !ChronoUnit.MILLIS.equals(e.getValue())) // millisecond assertions are skipped because it is not supported yet
                .forEach(e -> {
                    log.debug("Checking saved date addition {}...", e.getKey());
                    assertEquals(date.plus(2 * DIFFERENCE.longValue(), e.getValue()), saved.getAs(LocalDate.class, e.getKey()));
                    log.debug("Checking saved date difference {}...", e.getKey() + CALCULATED_DURATION_UNIT);
                    assertEquals(Double.valueOf(2 * DIFFERENCE), saved.getAs(Double.class, e.getKey() + CALCULATED_DURATION_UNIT));
                });

        testFixture.getDao().delete(dto, id);

        // TODO - check daylight saving (JNG-1586)
    }
}
