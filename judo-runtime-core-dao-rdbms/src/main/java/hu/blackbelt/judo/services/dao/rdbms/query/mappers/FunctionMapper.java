package hu.blackbelt.judo.services.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.dao.rdbms.Dialect;
import hu.blackbelt.judo.services.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.services.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.services.dao.rdbms.query.model.*;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EClass;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class FunctionMapper extends RdbmsMapper<Function> {

    @NonNull
    private RdbmsBuilder rdbmsBuilder;

    @Override
    public Stream<? extends RdbmsField> map(final Function function, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final EMap<ParameterName, RdbmsField> parameters = ECollections.asEMap(function.getParameters().stream()
                .collect(Collectors.toMap(
                        e -> e.getParameterName(),
                        e -> rdbmsBuilder.mapFeatureToRdbms(e.getParameterValue(), ancestors, parentIdFilterQuery, queryParameters).findAny().get())));

        final Dialect dialect = rdbmsBuilder.getDialect();

        return getTargets(function).map(t -> {
            RdbmsFunction.RdbmsFunctionBuilder builder = RdbmsFunction.builder()
                    .target(t.getTarget())
                    .targetAttribute(t.getTargetAttribute())
                    .alias(t.getAlias());

            switch (function.getSignature()) {
                case NOT: {
                    return builder.pattern("(NOT {0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.BOOLEAN)))
                            .build();
                }
                case AND: {
                    return builder.pattern("({0} AND {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case OR: {
                    return builder.pattern("({0} OR {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case XOR: {
                    return builder.pattern("(({0} AND NOT {1}) OR (NOT {0} AND {1}))")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case IMPLIES: {
                    return builder.pattern("(NOT {0} OR {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case GREATER_THAN: {
                    return builder.pattern("({0} > {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case GREATER_OR_EQUAL: {
                    return builder.pattern("({0} >= {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case EQUALS: {
                    return builder.pattern("({0} = {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case NOT_EQUALS: {
                    return builder.pattern("({0} <> {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case LESS_OR_EQUAL: {
                    return builder.pattern("({0} <= {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case LESS_THAN: {
                    return builder.pattern("({0} < {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case ADD_DECIMAL:
                case ADD_INTEGER: {
                    return builder.pattern("({0} + {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case SUBTRACT_DECIMAL:
                case SUBTRACT_INTEGER: {
                    return builder.pattern("({0} - {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case MULTIPLE_DECIMAL:
                case MULTIPLE_INTEGER: {
                    return builder.pattern(getDecimalType(function).map(type -> "CAST({0} * {1} AS " + type + ")").orElse("({0} * {1})"))
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case DIVIDE_INTEGER: {
                    return builder.pattern("FLOOR({0} / {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case DIVIDE_DECIMAL: {
                    return builder.pattern(getDecimalType(function).map(type -> "(CAST({0} as " + type + ") / {1})").orElse("(CAST({0} as DECIMAL(35,20)) / {1})"))
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case OPPOSITE_INTEGER:
                case OPPOSITE_DECIMAL: {
                    return builder.pattern("(0 - {0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.NUMBER)))
                            .build();
                }
                case ROUND_DECIMAL: {
                    return builder.pattern("ROUND({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.NUMBER)))
                            .build();
                }
                case MODULO_INTEGER: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("({0} % {1})");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("MOD({0}, {1})");
                    } else if (Dialect.JOOQ.equals(dialect)) {
                        builder.pattern("({0} % {1})");
                    } else {
                        throw new UnsupportedOperationException("MATCHES_STRING function is not supported in dialect: " + dialect);
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case LENGTH_STRING: {
                    return builder.pattern("LENGTH({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING)))
                            .build();
                }
                case LOWER_STRING: {
                    return builder.pattern("LOWER({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING)))
                            .build();
                }
                case TRIM_STRING: {
                    return builder.pattern("TRIM({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING)))
                            .build();
                }
                case INTEGER_TO_STRING:
                case DECIMAL_TO_STRING:
                case DATE_TO_STRING:
                case TIME_TO_STRING:
                case LOGICAL_TO_STRING:
                case ENUM_TO_STRING:
                case CUSTOM_TO_STRING: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("CAST({0} AS TEXT)");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("CAST({0} AS LONGVARCHAR)");
                    } else {
                        builder.pattern("CAST({0} AS VARCHAR(2000))");
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.PRIMITIVE)))
                            .build();
                }
                case TIMESTAMP_TO_STRING: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("REPLACE(CAST({0} AS TEXT), '' '', ''T'')");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("REPLACE(CAST({0} AS LONGVARCHAR), '' '', ''T'')");
                    } else {
                        builder.pattern("REPLACE(CAST({0} AS VARCHAR(2000)), '' '', ''T'')");
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.PRIMITIVE)))
                            .build();
                }
                case UPPER_STRING: {
                    return builder.pattern("UPPER({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING)))
                            .build();
                }
                case CONCATENATE_STRING: {
                    return builder.pattern("({0} || {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case LIKE: {
                    return builder.pattern("({0} LIKE {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING), parameters.get(ParameterName.PATTERN)))
                            .build();
                }
                case ILIKE: {
                    return builder.pattern("({0} ILIKE {1})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING), parameters.get(ParameterName.PATTERN)))
                            .build();
                }
                case MATCHES_STRING: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("({0} ~ {1})");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("REGEXP_MATCHES({0}, {1})");
                    } else if (Dialect.JOOQ.equals(dialect)) {
                        builder.pattern("({0} RLIKE {1})");
                    } else {
                        throw new UnsupportedOperationException("MATCHES_STRING function is not supported in dialect: " + dialect);
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING), parameters.get(ParameterName.PATTERN)))
                            .build();
                }
                case POSITION_STRING: {
                    return builder.pattern("POSITION({1} IN {0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING), parameters.get(ParameterName.CONTAINMENT)))
                            .build();
                }
                case REPLACE_STRING: {
                    return builder.pattern("REPLACE({0}, {1}, {2})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING), parameters.get(ParameterName.PATTERN), parameters.get(ParameterName.REPLACEMENT)))
                            .build();
                }
                case SUBSTRING_STRING: {
                    return builder.pattern("SUBSTRING({0}, CAST ({1} AS INTEGER), CAST({2} AS INTEGER))")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING), parameters.get(ParameterName.POSITION), parameters.get(ParameterName.LENGTH)))
                            .build();
                }
                case ADD_DATE: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("({0} + CAST({1} || '' days'' AS INTERVAL))");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("TIMESTAMPADD(SQL_TSI_DAY, {1}, {0})");
                    } else if (Dialect.JOOQ.equals(dialect)) {
                        builder.pattern("DATEADD(DAY, {1}, {0})::DATE");
                    } else {
                        throw new UnsupportedOperationException("ADD_DATE function is not supported in dialect: " + dialect);
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.DATE), parameters.get(ParameterName.ADDITION)))
                            .build();
                }
                case DIFFERENCE_DATE: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("(CAST({0} AS DATE) - CAST({1} AS DATE))");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("TIMESTAMPDIFF(SQL_TSI_DAY, {1}, {0})");
                    } else if (Dialect.JOOQ.equals(dialect)) {
                        builder.pattern("DATEDIFF({0}, {1})");
                    } else {
                        throw new UnsupportedOperationException("DIFFERENCE_TIMESTAMP function is not supported in dialect: " + dialect);
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.END), parameters.get(ParameterName.START)))
                            .build();
                }
                case ADD_TIMESTAMP: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("({0} + CAST({1} / 1000 || '' seconds'' AS INTERVAL))");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("TIMESTAMPADD(SQL_TSI_MILLI_SECOND, MOD({1}, 1000), TIMESTAMPADD(SQL_TSI_SECOND, {1} / 1000, {0}))");
                    } else if (Dialect.JOOQ.equals(dialect)) {
                        builder.pattern("DATEADD(SECOND, {1} / 1000, {0})");
                    } else {
                        throw new UnsupportedOperationException("ADD_TIMESTAMP function is not supported in dialect: " + dialect);
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP), parameters.get(ParameterName.ADDITION)))
                            .build();
                }
                case ADD_TIME: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("({0} + CAST({1} / 1000 || '' seconds'' AS INTERVAL))");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("TIMESTAMPADD(SQL_TSI_MILLI_SECOND, MOD({1}, 1000), TIMESTAMPADD(SQL_TSI_SECOND, {1} / 1000, CAST({0} AS TIMESTAMP)))");
                    } else if (Dialect.JOOQ.equals(dialect)) {
                        builder.pattern("DATEADD(SECOND, {1} / 1000, {0})");
                    } else {
                        throw new UnsupportedOperationException("ADD_TIME function is not supported in dialect: " + dialect);
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIME), parameters.get(ParameterName.ADDITION)))
                            .build();
                }
                case DIFFERENCE_TIMESTAMP: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("(EXTRACT(EPOCH FROM {0} - {1}) * 1000)");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("TIMESTAMPDIFF(SQL_TSI_MILLI_SECOND, {1}, {0})");
                    } else if (Dialect.JOOQ.equals(dialect)) {
                        builder.pattern("TIMESTAMPDIFF({0}, {1})");
                    } else {
                        throw new UnsupportedOperationException("DIFFERENCE_TIMESTAMP function is not supported in dialect: " + dialect);
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.END), parameters.get(ParameterName.START)))
                            .build();
                }
                case DIFFERENCE_TIME: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("(EXTRACT(EPOCH FROM {0} - {1}) * 1000)");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("TIMESTAMPDIFF(SQL_TSI_MILLI_SECOND, CAST({1} AS TIMESTAMP), CAST({0} AS TIMESTAMP))");
                    } else if (Dialect.JOOQ.equals(dialect)) {
                        builder.pattern("TIMESTAMPDIFF({0}, {1})");
                    } else {
                        throw new UnsupportedOperationException("DIFFERENCE_TIME function is not supported in dialect: " + dialect);
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.END), parameters.get(ParameterName.START)))
                            .build();
                }

                case IS_UNDEFINED_ATTRIBUTE: {
                    return builder.pattern("({0} IS NULL)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.ATTRIBUTE)))
                            .build();
                }
                case IS_UNDEFINED_OBJECT: {
                    return builder.pattern("({0} IS NULL)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.RELATION)))
                            .build();
                }
                case INSTANCE_OF: {
                    return builder.pattern("EXISTS (SELECT 1 FROM {1} WHERE " + StatementExecutor.ID_COLUMN_NAME + " = {0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.INSTANCE), parameters.get(ParameterName.TYPE)))
                            .build();
                }
                case TYPE_OF: {
                    return builder.pattern("EXISTS (SELECT 1 FROM {1} WHERE " + StatementExecutor.ID_COLUMN_NAME + " = {0} AND " + StatementExecutor.ENTITY_TYPE_COLUMN_NAME + " = {2})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.INSTANCE), parameters.get(ParameterName.TYPE), RdbmsConstant.builder()
                                    .parameter(rdbmsBuilder.getParameterMapper().createParameter(AsmUtils.getClassifierFQName(((RdbmsEntityTypeName) parameters.get(ParameterName.TYPE)).getType()), null))
                                    .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                                    .build()))
                            .build();
                }
                case MEMBER_OF: {
                    return builder.pattern("({0} IN ({1}))")
                            .parameters(Arrays.asList(parameters.get(ParameterName.INSTANCE), parameters.get(ParameterName.COLLECTION)))
                            .build();
                }
                case EXISTS: {
                    return builder.pattern("EXISTS ({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.COLLECTION)))
                            .build();
                }
                case NOT_EXISTS: {
                    return builder.pattern("(NOT EXISTS ({0}))")
                            .parameters(Arrays.asList(parameters.get(ParameterName.COLLECTION)))
                            .build();
                }
                case COUNT: {
                    return builder.pattern("COUNT(DISTINCT {0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.ITEM)))
                            .build();
                }
                case SUM_INTEGER:
                case SUM_DECIMAL: {
                    return builder.pattern("SUM({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.NUMBER)))
                            .build();
                }
                case MIN_INTEGER:
                case MIN_DECIMAL: {
                    return builder.pattern("MIN({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.NUMBER)))
                            .build();
                }
                case MIN_STRING: {
                    return builder.pattern("MIN({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING)))
                            .build();
                }
                case MIN_DATE: {
                    return builder.pattern("MIN({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.DATE)))
                            .build();
                }
                case MIN_TIMESTAMP: {
                    return builder.pattern("MIN({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP)))
                            .build();
                }
                case MIN_TIME: {
                    return builder.pattern("MIN({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIME)))
                            .build();
                }
                case MAX_INTEGER:
                case MAX_DECIMAL: {
                    return builder.pattern("MAX({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.NUMBER)))
                            .build();
                }
                case MAX_STRING: {
                    return builder.pattern("MAX({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.STRING)))
                            .build();
                }
                case MAX_DATE: {
                    return builder.pattern("MAX({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.DATE)))
                            .build();
                }
                case MAX_TIMESTAMP: {
                    return builder.pattern("MAX({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP)))
                            .build();
                }
                case MAX_TIME: {
                    return builder.pattern("MAX({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIME)))
                            .build();
                }
                case AVG_DECIMAL: {
                    return builder.pattern("AVG({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.NUMBER)))
                            .build();
                }
                case AVG_DATE: {
                    return builder.pattern("AVG({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.DATE)))
                            .build();
                }
                case AVG_TIMESTAMP: {
                    return builder.pattern("AVG({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP)))
                            .build();
                }
                case AVG_TIME: {
                    return builder.pattern("AVG({0})")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIME)))
                            .build();
                }
                case CASE_WHEN: {
                    return builder.pattern("CASE WHEN {0} THEN {1} ELSE {2} END")
                            .parameters(Arrays.asList(parameters.get(ParameterName.CONDITION), parameters.get(ParameterName.LEFT), parameters.get(ParameterName.RIGHT)))
                            .build();
                }
                case UNDEFINED: {
                    return builder.pattern("NULL")
                            .build();
                }
                case YEARS_OF_DATE: {
                    return builder.pattern("CAST(EXTRACT(YEAR from CAST({0} AS DATE)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.DATE)))
                            .build();
                }
                case MONTHS_OF_DATE: {
                    return builder.pattern("CAST(EXTRACT(MONTH from CAST({0} AS DATE)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.DATE)))
                            .build();
                }
                case DAYS_OF_DATE: {
                    return builder.pattern("CAST(EXTRACT(DAY from CAST({0} AS DATE)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.DATE)))
                            .build();
                }
                case YEARS_OF_TIMESTAMP: {
                    return builder.pattern("CAST(EXTRACT(YEAR from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP)))
                            .build();
                }
                case MONTHS_OF_TIMESTAMP: {
                    return builder.pattern("CAST(EXTRACT(MONTH from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP)))
                            .build();
                }
                case DAYS_OF_TIMESTAMP: {
                    return builder.pattern("CAST(EXTRACT(DAY from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP)))
                            .build();
                }
                case HOURS_OF_TIMESTAMP: {
                    return builder.pattern("CAST(EXTRACT(HOUR from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP)))
                            .build();
                }
                case MINUTES_OF_TIMESTAMP: {
                    return builder.pattern("CAST(EXTRACT(MINUTE from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP)))
                            .build();
                }
                case SECONDS_OF_TIMESTAMP: {
                    return builder.pattern("CAST(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP)))
                            .build();
                }
                case MILLISECONDS_OF_TIMESTAMP: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("(CAST(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) * 1000 AS INTEGER) % 1000)");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("MOD(CAST(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) * 1000 AS INTEGER), 1000)");
                    } else if (Dialect.JOOQ.equals(dialect)) {
                        builder.pattern("(CAST(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) * 1000 AS INTEGER) % 1000)");
                    } else {
                        throw new UnsupportedOperationException("EXTRACT_TIMESTAMP function is not supported in dialect: " + dialect);
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIMESTAMP)))
                            .build();
                }
                case HOURS_OF_TIME: {
                    return builder.pattern("CAST(EXTRACT(HOUR from CAST({0} AS TIME)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIME)))
                            .build();
                }
                case MINUTES_OF_TIME: {
                    return builder.pattern("CAST(EXTRACT(MINUTE from CAST({0} AS TIME)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIME)))
                            .build();
                }
                case SECONDS_OF_TIME: {
                    return builder.pattern("CAST(EXTRACT(SECOND from CAST({0} AS TIME)) AS INTEGER)")
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIME)))
                            .build();
                }
                case MILLISECONDS_OF_TIME: {
                    if (Dialect.POSTGRESQL.equals(dialect)) {
                        builder.pattern("(CAST(EXTRACT(SECOND from CAST({0} AS TIME)) * 1000 AS INTEGER) % 1000)");
                    } else if (Dialect.HSQLDB.equals(dialect)) {
                        builder.pattern("MOD(CAST(EXTRACT(SECOND from CAST({0} AS TIME)) * 1000 AS INTEGER), 1000)");
                    } else if (Dialect.JOOQ.equals(dialect)) {
                        builder.pattern("(CAST(EXTRACT(SECOND from CAST({0} AS TIME)) * 1000 AS INTEGER) % 1000)");
                    } else {
                        throw new UnsupportedOperationException("EXTRACT_TIME function is not supported in dialect: " + dialect);
                    }
                    return builder
                            .parameters(Arrays.asList(parameters.get(ParameterName.TIME)))
                            .build();
                }
                case TO_DATE: {
                    return builder.pattern("CAST(TO_DATE(CAST({0} AS INTEGER) || ''-'' || CAST({1} AS  INTEGER) || ''-'' || CAST({2} AS INTEGER), ''YYYY-MM-DD'') AS DATE)")
                            .parameters(Arrays.asList(
                                    parameters.get(ParameterName.YEAR),
                                    parameters.get(ParameterName.MONTH),
                                    parameters.get(ParameterName.DAY)
                            )).build();
                }
                case TO_TIMESTAMP: {
                    return builder.pattern("CAST(TO_TIMESTAMP(CAST({0} AS INTEGER) || ''-'' || CAST({1} AS INTEGER) || ''-'' || CAST({2} AS INTEGER) || '' '' || CAST({3} AS INTEGER) || '':'' || CAST({4} AS INTEGER) || '':'' || CAST({5} AS DECIMAL), ''YYYY-MM-DD HH24:MI:SS'') AS TIMESTAMP)")
                            .parameters(Arrays.asList(
                                    parameters.get(ParameterName.YEAR),
                                    parameters.get(ParameterName.MONTH),
                                    parameters.get(ParameterName.DAY),
                                    parameters.get(ParameterName.HOUR),
                                    parameters.get(ParameterName.MINUTE),
                                    parameters.get(ParameterName.SECOND)
                            )).build();
                }
                case TO_TIME: {
                    return builder.pattern("CAST(CAST({0} AS INTEGER) || '':'' || CAST({1} AS INTEGER) || '':'' || CAST({2} AS INTEGER) AS TIME)")
                            .parameters(Arrays.asList(
                                    parameters.get(ParameterName.HOUR),
                                    parameters.get(ParameterName.MINUTE),
                                    parameters.get(ParameterName.SECOND)
                            )).build();
                }
                default:
                    throw new UnsupportedOperationException("Unsupported function: " + function.getSignature());
            }
        });
    }

    private Optional<String> getDecimalType(Function function) {
        final Optional<Integer> precision = function.getConstraints().stream()
                                                    .filter(c -> ResultConstraint.PRECISION.equals(c.getResultConstraint()))
                                                    .map(c -> Integer.parseInt(c.getValue()))
                                                    .findAny();
        final Optional<Integer> scale = function.getConstraints().stream()
                                                .filter(c -> ResultConstraint.SCALE.equals(c.getResultConstraint()))
                                                .map(c -> Integer.parseInt(c.getValue()))
                                                .findAny();
        return precision.flatMap(p -> scale.map(s -> String.format("DECIMAL(%d,%d)", p, s)));
    }
}
