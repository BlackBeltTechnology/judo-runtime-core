package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsConstant;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsEntityTypeName;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsFunction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FunctionMapper extends RdbmsMapper<Function> {

    @NonNull
    private RdbmsBuilder rdbmsBuilder;

    private final Map<FunctionSignature, java.util.function.Function<FunctionContext, RdbmsFunction>> functionMap = new LinkedHashMap<>();

    @AllArgsConstructor
    @Builder
    static class FunctionContext {
        EMap<ParameterName, RdbmsField> parameters;
        RdbmsFunction.RdbmsFunctionBuilder builder;
        Function function;
    }

    public FunctionMapper(@NonNull RdbmsBuilder rdbmsBuilder) {
        this.rdbmsBuilder = rdbmsBuilder;

        functionMap.put(FunctionSignature.NOT, c ->
                c.builder.pattern("(NOT {0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.BOOLEAN)))
                        .build());

        functionMap.put(FunctionSignature.AND, c ->
                c.builder.pattern("({0} AND {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.OR, c ->
                c.builder.pattern("({0} OR {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.XOR, c ->
                c.builder.pattern("(({0} AND NOT {1}) OR (NOT {0} AND {1}))")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.IMPLIES, c ->
                c.builder.pattern("(NOT {0} OR {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.GREATER_THAN, c ->
                c.builder.pattern("({0} > {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.GREATER_OR_EQUAL, c ->
                c.builder.pattern("({0} >= {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.EQUALS, c ->
                c.builder.pattern("({0} = {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.NOT_EQUALS, c ->
                c.builder.pattern("({0} <> {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.LESS_OR_EQUAL, c ->
                c.builder.pattern("({0} <= {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.LESS_THAN, c ->
                c.builder.pattern("({0} < {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.ADD_DECIMAL, c ->
                c.builder.pattern("({0} + {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());
        functionMap.put(FunctionSignature.ADD_INTEGER, functionMap.get(FunctionSignature.ADD_DECIMAL));

        functionMap.put(FunctionSignature.SUBTRACT_DECIMAL, c ->
                c.builder.pattern("({0} - {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());
        functionMap.put(FunctionSignature.SUBTRACT_INTEGER, functionMap.get(FunctionSignature.SUBTRACT_DECIMAL));

        functionMap.put(FunctionSignature.MULTIPLE_DECIMAL, c ->
                c.builder.pattern(getDecimalType(c.function).map(type -> "CAST({0} * {1} AS " + type + ")").orElse("({0} * {1})"))
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());
        functionMap.put(FunctionSignature.MULTIPLE_INTEGER, functionMap.get(FunctionSignature.MULTIPLE_DECIMAL));

        functionMap.put(FunctionSignature.DIVIDE_INTEGER, c ->
                c.builder.pattern("FLOOR({0} / {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.DIVIDE_DECIMAL, c ->
                c.builder.pattern(getDecimalType(c.function).map(type -> "(CAST({0} as " + type + ") / {1})").orElse("(CAST({0} as DECIMAL(35,20)) / {1})"))
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.OPPOSITE_INTEGER, c ->
                c.builder.pattern("(0 - {0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.NUMBER)))
                        .build());
        functionMap.put(FunctionSignature.OPPOSITE_DECIMAL, functionMap.get(FunctionSignature.OPPOSITE_INTEGER));

        functionMap.put(FunctionSignature.ROUND_DECIMAL, c ->
                c.builder.pattern("ROUND({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.NUMBER)))
                        .build());

        /*
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

         */
        functionMap.put(FunctionSignature.MODULO_INTEGER, c ->
                c.builder.pattern("MOD({0}, {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.LENGTH_STRING, c ->
                c.builder.pattern("LENGTH({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING)))
                        .build());

        functionMap.put(FunctionSignature.LOWER_STRING, c ->
                c.builder.pattern("LOWER({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING)))
                        .build());

        functionMap.put(FunctionSignature.TRIM_STRING, c ->
                c.builder.pattern("TRIM({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING)))
                        .build());



        /*
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

         */
        functionMap.put(FunctionSignature.INTEGER_TO_STRING, c ->
                c.builder.pattern("CAST({0} AS LONGVARCHAR)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.PRIMITIVE)))
                        .build());
        functionMap.put(FunctionSignature.DECIMAL_TO_STRING, functionMap.get(FunctionSignature.INTEGER_TO_STRING));
        functionMap.put(FunctionSignature.DATE_TO_STRING, functionMap.get(FunctionSignature.INTEGER_TO_STRING));
        functionMap.put(FunctionSignature.TIME_TO_STRING, functionMap.get(FunctionSignature.INTEGER_TO_STRING));
        functionMap.put(FunctionSignature.LOGICAL_TO_STRING, functionMap.get(FunctionSignature.INTEGER_TO_STRING));
        functionMap.put(FunctionSignature.ENUM_TO_STRING, functionMap.get(FunctionSignature.INTEGER_TO_STRING));
        functionMap.put(FunctionSignature.CUSTOM_TO_STRING, functionMap.get(FunctionSignature.INTEGER_TO_STRING));


        /*
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

         */
        functionMap.put(FunctionSignature.TIMESTAMP_TO_STRING, c ->
                c.builder.pattern("REPLACE(CAST({0} AS LONGVARCHAR), '' '', ''T'')")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.PRIMITIVE)))
                        .build());

        functionMap.put(FunctionSignature.UPPER_STRING, c ->
                c.builder.pattern("UPPER({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING)))
                        .build());

        functionMap.put(FunctionSignature.CONCATENATE_STRING, c ->
                c.builder.pattern("({0} || {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.LIKE, c ->
                c.builder.pattern("({0} LIKE {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN)))
                        .build());

        functionMap.put(FunctionSignature.LIKE, c ->
                c.builder.pattern("({0} LIKE {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN)))
                        .build());

        functionMap.put(FunctionSignature.ILIKE, c ->
                c.builder.pattern("({0} ILIKE {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN)))
                        .build());

        /*
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

         */

        functionMap.put(FunctionSignature.MATCHES_STRING, c ->
                c.builder.pattern("REGEXP_MATCHES({0}, {1})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN)))
                        .build());

        functionMap.put(FunctionSignature.POSITION_STRING, c ->
                c.builder.pattern("POSITION({1} IN {0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.CONTAINMENT)))
                        .build());

        functionMap.put(FunctionSignature.REPLACE_STRING, c ->
                c.builder.pattern("REPLACE({0}, {1}, {2})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN), c.parameters.get(ParameterName.REPLACEMENT)))
                        .build());

        functionMap.put(FunctionSignature.SUBSTRING_STRING, c ->
                c.builder.pattern("SUBSTRING({0}, CAST ({1} AS INTEGER), CAST({2} AS INTEGER))")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.POSITION), c.parameters.get(ParameterName.LENGTH)))
                        .build());

        /*
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

         */
        functionMap.put(FunctionSignature.ADD_DATE, c ->
                c.builder.pattern("TIMESTAMPADD(SQL_TSI_DAY, {1}, {0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.DATE), c.parameters.get(ParameterName.ADDITION)))
                        .build());

        /*
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

         */
        functionMap.put(FunctionSignature.DIFFERENCE_DATE, c ->
                c.builder.pattern("TIMESTAMPDIFF(SQL_TSI_DAY, {1}, {0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START)))
                        .build());

        /*
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

         */
        functionMap.put(FunctionSignature.ADD_TIMESTAMP, c ->
                c.builder.pattern("TIMESTAMPADD(SQL_TSI_MILLI_SECOND, MOD({1}, 1000), TIMESTAMPADD(SQL_TSI_SECOND, {1} / 1000, {0}))")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP), c.parameters.get(ParameterName.ADDITION)))
                        .build());

        /*
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
         */
        functionMap.put(FunctionSignature.ADD_TIME, c ->
                c.builder.pattern("TIMESTAMPADD(SQL_TSI_MILLI_SECOND, MOD({1}, 1000), TIMESTAMPADD(SQL_TSI_SECOND, {1} / 1000, CAST({0} AS TIMESTAMP)))")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIME), c.parameters.get(ParameterName.ADDITION)))
                        .build());

        /*
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

         */
        functionMap.put(FunctionSignature.DIFFERENCE_TIMESTAMP, c ->
                c.builder.pattern("TIMESTAMPDIFF(SQL_TSI_MILLI_SECOND, {1}, {0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START)))
                        .build());


        /*
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
         */
        functionMap.put(FunctionSignature.DIFFERENCE_TIME, c ->
                c.builder.pattern("TIMESTAMPDIFF(SQL_TSI_MILLI_SECOND, CAST({1} AS TIMESTAMP), CAST({0} AS TIMESTAMP))")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START)))
                        .build());

        /*

         */
        functionMap.put(FunctionSignature.IS_UNDEFINED_ATTRIBUTE, c ->
                c.builder.pattern("({0} IS NULL)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.ATTRIBUTE)))
                        .build());

        functionMap.put(FunctionSignature.IS_UNDEFINED_OBJECT, c ->
                c.builder.pattern("({0} IS NULL)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.RELATION)))
                        .build());

        functionMap.put(FunctionSignature.INSTANCE_OF, c ->
                c.builder.pattern("EXISTS (SELECT 1 FROM {1} WHERE " + StatementExecutor.ID_COLUMN_NAME + " = {0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.INSTANCE), c.parameters.get(ParameterName.TYPE)))
                        .build());

        functionMap.put(FunctionSignature.TYPE_OF, c ->
                c.builder.pattern("EXISTS (SELECT 1 FROM {1} WHERE " + StatementExecutor.ID_COLUMN_NAME + " = {0} AND " + StatementExecutor.ENTITY_TYPE_COLUMN_NAME + " = {2})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.INSTANCE), c.parameters.get(ParameterName.TYPE), RdbmsConstant.builder()
                                .parameter(rdbmsBuilder.getParameterMapper().createParameter(AsmUtils.getClassifierFQName(((RdbmsEntityTypeName) c.parameters.get(ParameterName.TYPE)).getType()), null))
                                .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                                .build()))
                        .build());

        functionMap.put(FunctionSignature.MEMBER_OF, c ->
                c.builder.pattern("({0} IN ({1}))")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.INSTANCE), c.parameters.get(ParameterName.COLLECTION)))
                        .build());

        functionMap.put(FunctionSignature.EXISTS, c ->
                c.builder.pattern("EXISTS ({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.COLLECTION)))
                        .build());

        functionMap.put(FunctionSignature.NOT_EXISTS, c ->
                c.builder.pattern("(NOT EXISTS ({0}))")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.COLLECTION)))
                        .build());

        functionMap.put(FunctionSignature.COUNT, c ->
                c.builder.pattern("COUNT(DISTINCT {0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.ITEM)))
                        .build());

        functionMap.put(FunctionSignature.SUM_INTEGER, c ->
                c.builder.pattern("SUM({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.NUMBER)))
                        .build());
        functionMap.put(FunctionSignature.SUM_DECIMAL, functionMap.get(FunctionSignature.SUM_INTEGER));

        functionMap.put(FunctionSignature.MIN_INTEGER, c ->
                c.builder.pattern("MIN({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.NUMBER)))
                        .build());
        functionMap.put(FunctionSignature.MIN_DECIMAL, functionMap.get(FunctionSignature.MIN_INTEGER));

        functionMap.put(FunctionSignature.MIN_STRING, c ->
                c.builder.pattern("MIN({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING)))
                        .build());

        functionMap.put(FunctionSignature.MIN_DATE, c ->
                c.builder.pattern("MIN({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.DATE)))
                        .build());

        functionMap.put(FunctionSignature.MIN_TIMESTAMP, c ->
                c.builder.pattern("MIN({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP)))
                        .build());

        functionMap.put(FunctionSignature.MIN_TIME, c ->
                c.builder.pattern("MIN({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIME)))
                        .build());

        functionMap.put(FunctionSignature.MAX_INTEGER, c ->
                c.builder.pattern("MAX({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.NUMBER)))
                        .build());
        functionMap.put(FunctionSignature.MAX_DECIMAL, functionMap.get(FunctionSignature.MAX_INTEGER));

        functionMap.put(FunctionSignature.MAX_STRING, c ->
                c.builder.pattern("MAX({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.STRING)))
                        .build());

        functionMap.put(FunctionSignature.MAX_DATE, c ->
                c.builder.pattern("MAX({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.DATE)))
                        .build());

        functionMap.put(FunctionSignature.MAX_TIMESTAMP, c ->
                c.builder.pattern("MAX({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP)))
                        .build());

        functionMap.put(FunctionSignature.MAX_TIME, c ->
                c.builder.pattern("MAX({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIME)))
                        .build());

        functionMap.put(FunctionSignature.AVG_DECIMAL, c ->
                c.builder.pattern("AVG({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.NUMBER)))
                        .build());

        functionMap.put(FunctionSignature.AVG_DATE, c ->
                c.builder.pattern("AVG({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.DATE)))
                        .build());

        functionMap.put(FunctionSignature.AVG_TIMESTAMP, c ->
                c.builder.pattern("AVG({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP)))
                        .build());

        functionMap.put(FunctionSignature.AVG_TIME, c ->
                c.builder.pattern("AVG({0})")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIME)))
                        .build());

        functionMap.put(FunctionSignature.CASE_WHEN, c ->
                c.builder.pattern("CASE WHEN {0} THEN {1} ELSE {2} END")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.CONDITION), c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT)))
                        .build());

        functionMap.put(FunctionSignature.UNDEFINED, c ->
                c.builder.pattern("NULL").build());

        functionMap.put(FunctionSignature.YEARS_OF_DATE, c ->
                c.builder.pattern("CAST(EXTRACT(YEAR from CAST({0} AS DATE)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.DATE)))
                        .build());

        functionMap.put(FunctionSignature.MONTHS_OF_DATE, c ->
                c.builder.pattern("CAST(EXTRACT(MONTH from CAST({0} AS DATE)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.DATE)))
                        .build());

        functionMap.put(FunctionSignature.DAYS_OF_DATE, c ->
                c.builder.pattern("CAST(EXTRACT(DAY from CAST({0} AS DATE)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.DATE)))
                        .build());

        functionMap.put(FunctionSignature.YEARS_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(YEAR from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP)))
                        .build());

        functionMap.put(FunctionSignature.MONTHS_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(MONTH from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP)))
                        .build());

        functionMap.put(FunctionSignature.DAYS_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(DAY from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP)))
                        .build());

        functionMap.put(FunctionSignature.HOURS_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(HOUR from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP)))
                        .build());

        functionMap.put(FunctionSignature.MINUTES_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(MINUTE from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP)))
                        .build());

        functionMap.put(FunctionSignature.SECONDS_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP)))
                        .build());

        /*
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

         */

        functionMap.put(FunctionSignature.MILLISECONDS_OF_TIMESTAMP, c ->
                c.builder.pattern("MOD(CAST(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) * 1000 AS INTEGER), 1000)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIMESTAMP)))
                        .build());

        functionMap.put(FunctionSignature.HOURS_OF_TIME, c ->
                c.builder.pattern("CAST(EXTRACT(HOUR from CAST({0} AS TIME)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIME)))
                        .build());

        functionMap.put(FunctionSignature.MINUTES_OF_TIME, c ->
                c.builder.pattern("CAST(EXTRACT(MINUTE from CAST({0} AS TIME)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIME)))
                        .build());

        functionMap.put(FunctionSignature.SECONDS_OF_TIME, c ->
                c.builder.pattern("CAST(EXTRACT(SECOND from CAST({0} AS TIME)) AS INTEGER)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIME)))
                        .build());

        /*
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

         */
        functionMap.put(FunctionSignature.MILLISECONDS_OF_TIME, c ->
                c.builder.pattern("MOD(CAST(EXTRACT(SECOND from CAST({0} AS TIME)) * 1000 AS INTEGER), 1000)")
                        .parameters(Arrays.asList(c.parameters.get(ParameterName.TIME)))
                        .build());

        functionMap.put(FunctionSignature.TO_DATE, c ->
                c.builder.pattern("CAST(TO_DATE(CAST({0} AS INTEGER) || ''-'' || CAST({1} AS  INTEGER) || ''-'' || CAST({2} AS INTEGER), ''YYYY-MM-DD'') AS DATE)")
                        .parameters(Arrays.asList(
                                c.parameters.get(ParameterName.YEAR),
                                c.parameters.get(ParameterName.MONTH),
                                c.parameters.get(ParameterName.DAY)
                        )).build());

        functionMap.put(FunctionSignature.TO_TIMESTAMP, c ->
                c.builder.pattern("CAST(TO_TIMESTAMP(CAST({0} AS INTEGER) || ''-'' || CAST({1} AS INTEGER) || ''-'' || CAST({2} AS INTEGER) || '' '' || CAST({3} AS INTEGER) || '':'' || CAST({4} AS INTEGER) || '':'' || CAST({5} AS DECIMAL), ''YYYY-MM-DD HH24:MI:SS'') AS TIMESTAMP)")
                        .parameters(Arrays.asList(
                                c.parameters.get(ParameterName.YEAR),
                                c.parameters.get(ParameterName.MONTH),
                                c.parameters.get(ParameterName.DAY),
                                c.parameters.get(ParameterName.HOUR),
                                c.parameters.get(ParameterName.MINUTE),
                                c.parameters.get(ParameterName.SECOND)
                        )).build());


        functionMap.put(FunctionSignature.TO_TIME, c ->
                c.builder.pattern("CAST(CAST({0} AS INTEGER) || '':'' || CAST({1} AS INTEGER) || '':'' || CAST({2} AS INTEGER) AS TIME)")
                        .parameters(Arrays.asList(
                                c.parameters.get(ParameterName.HOUR),
                                c.parameters.get(ParameterName.MINUTE),
                                c.parameters.get(ParameterName.SECOND)
                        )).build());

    }

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

            java.util.function.Function<FunctionContext, RdbmsFunction> func = functionMap.get(function.getSignature());
            if (func == null) {
                throw new UnsupportedOperationException("Unsupported function: " + function.getSignature());
            }

            return func.apply(FunctionContext.builder()
                    .function(function)
                    .parameters(parameters)
                    .builder(builder)
                    .build());


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
