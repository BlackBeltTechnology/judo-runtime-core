package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsConstant;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsEntityTypeName;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsFunction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class FunctionMapper<ID> extends RdbmsMapper<Function> {

    private final RdbmsBuilder<ID> rdbmsBuilder;

    @Getter
    private final Map<FunctionSignature, java.util.function.Function<FunctionContext, RdbmsFunction.RdbmsFunctionBuilder>> functionBuilderMap = new LinkedHashMap<>();

    @AllArgsConstructor
    @Builder
    public static class FunctionContext {
        public EMap<ParameterName, RdbmsField> parameters;
        public RdbmsFunction.RdbmsFunctionBuilder builder;
        public Function function;
    }

    public FunctionMapper(@NonNull RdbmsBuilder rdbmsBuilder) {
        this.rdbmsBuilder = rdbmsBuilder;

        functionBuilderMap.put(FunctionSignature.NOT, c ->
                c.builder.pattern("(NOT {0})").parameters(
                        List.of(c.parameters.get(ParameterName.BOOLEAN))));

        functionBuilderMap.put(FunctionSignature.AND, c ->
                c.builder.pattern("({0} AND {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.OR, c ->
                c.builder.pattern("({0} OR {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.XOR, c ->
                c.builder.pattern("(({0} AND NOT {1}) OR (NOT {0} AND {1}))")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.IMPLIES, c ->
                c.builder.pattern("(NOT {0} OR {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.GREATER_THAN, c ->
                c.builder.pattern("({0} > {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.GREATER_OR_EQUAL, c ->
                c.builder.pattern("({0} >= {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.EQUALS, c ->
                c.builder.pattern("({0} = {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.NOT_EQUALS, c ->
                c.builder.pattern("({0} <> {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.LESS_OR_EQUAL, c ->
                c.builder.pattern("({0} <= {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.LESS_THAN, c ->
                c.builder.pattern("({0} < {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.ADD_DECIMAL, c ->
                c.builder.pattern("({0} + {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));
        functionBuilderMap.put(FunctionSignature.ADD_INTEGER, functionBuilderMap.get(FunctionSignature.ADD_DECIMAL));

        functionBuilderMap.put(FunctionSignature.SUBTRACT_DECIMAL, c ->
                c.builder.pattern("({0} - {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.SUBTRACT_INTEGER, functionBuilderMap.get(FunctionSignature.SUBTRACT_DECIMAL));

        functionBuilderMap.put(FunctionSignature.MULTIPLE_DECIMAL, c ->
                c.builder.pattern(getDecimalType(c.function).map(type -> "CAST({0} * {1} AS " + type + ")").orElse("({0} * {1})"))
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));
        functionBuilderMap.put(FunctionSignature.MULTIPLE_INTEGER, functionBuilderMap.get(FunctionSignature.MULTIPLE_DECIMAL));

        functionBuilderMap.put(FunctionSignature.DIVIDE_INTEGER, c ->
                c.builder.pattern("FLOOR({0} / {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.DIVIDE_DECIMAL, c ->
                c.builder.pattern(getDecimalType(c.function).map(type -> "(CAST({0} as " + type + ") / {1})").orElse("(CAST({0} as DECIMAL(35,20)) / {1})"))
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.OPPOSITE_INTEGER, c ->
                c.builder.pattern("(0 - {0})")
                        .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));
        functionBuilderMap.put(FunctionSignature.OPPOSITE_DECIMAL, functionBuilderMap.get(FunctionSignature.OPPOSITE_INTEGER));

        functionBuilderMap.put(FunctionSignature.ROUND_DECIMAL, c ->
                c.builder.pattern("ROUND({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));

        functionBuilderMap.put(FunctionSignature.ABSOLUTE_NUMERIC, c ->
                c.builder.pattern("ABS({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));

        functionBuilderMap.put(FunctionSignature.CEIL_NUMERIC, c ->
                c.builder.pattern("CEIL({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));

        functionBuilderMap.put(FunctionSignature.FLOOR_NUMERIC, c ->
                c.builder.pattern("FLOOR({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));

        functionBuilderMap.put(FunctionSignature.MODULO_INTEGER, c ->
                c.builder.pattern("MOD({0}, {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.LENGTH_STRING, c ->
                c.builder.pattern("LENGTH({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING))));

        functionBuilderMap.put(FunctionSignature.LOWER_STRING, c ->
                c.builder.pattern("LOWER({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING))));

        functionBuilderMap.put(FunctionSignature.TRIM_STRING, c ->
                c.builder.pattern("TRIM({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING))));

        functionBuilderMap.put(FunctionSignature.LEFT_TRIM_STRING, c ->
                c.builder.pattern("LTRIM({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING))));

        functionBuilderMap.put(FunctionSignature.RIGHT_TRIM_STRING, c ->
                c.builder.pattern("RTRIM({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING))));

        functionBuilderMap.put(FunctionSignature.INTEGER_TO_STRING, c ->
                c.builder.pattern("CAST({0} AS LONGVARCHAR)")
                        .parameters(List.of(c.parameters.get(ParameterName.PRIMITIVE))));
        functionBuilderMap.put(FunctionSignature.DECIMAL_TO_STRING, functionBuilderMap.get(FunctionSignature.INTEGER_TO_STRING));
        functionBuilderMap.put(FunctionSignature.DATE_TO_STRING, functionBuilderMap.get(FunctionSignature.INTEGER_TO_STRING));
        functionBuilderMap.put(FunctionSignature.TIME_TO_STRING, functionBuilderMap.get(FunctionSignature.INTEGER_TO_STRING));
        functionBuilderMap.put(FunctionSignature.LOGICAL_TO_STRING, functionBuilderMap.get(FunctionSignature.INTEGER_TO_STRING));
        functionBuilderMap.put(FunctionSignature.ENUM_TO_STRING, functionBuilderMap.get(FunctionSignature.INTEGER_TO_STRING));
        functionBuilderMap.put(FunctionSignature.CUSTOM_TO_STRING, functionBuilderMap.get(FunctionSignature.INTEGER_TO_STRING));

        functionBuilderMap.put(FunctionSignature.TIMESTAMP_TO_STRING, c ->
                c.builder.pattern("REPLACE(CAST({0} AS LONGVARCHAR), '' '', ''T'')")
                        .parameters(List.of(c.parameters.get(ParameterName.PRIMITIVE))));

        functionBuilderMap.put(FunctionSignature.UPPER_STRING, c ->
                c.builder.pattern("UPPER({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING))));

//        functionBuilderMap.put(FunctionSignature.CAPITALIZE_STRING, c ->
//                c.builder.pattern("(UPPER(SUBSTRING({0}, 1, 1)) || LOWER(SUBSTRING({0}, 2)))")
//                        .parameters(List.of(c.parameters.get(ParameterName.STRING))));

        functionBuilderMap.put(FunctionSignature.CONCATENATE_STRING, c ->
                c.builder.pattern("({0} || {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.LIKE, c ->
                c.builder.pattern("({0} LIKE {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN))));

        functionBuilderMap.put(FunctionSignature.ILIKE, c ->
                c.builder.pattern("({0} ILIKE {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN))));

        functionBuilderMap.put(FunctionSignature.MATCHES_STRING, c ->
                c.builder.pattern("REGEXP_MATCHES({0}, {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN))));

        functionBuilderMap.put(FunctionSignature.POSITION_STRING, c ->
                c.builder.pattern("POSITION({1} IN {0})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.CONTAINMENT))));

        functionBuilderMap.put(FunctionSignature.REPLACE_STRING, c ->
                c.builder.pattern("REPLACE({0}, {1}, {2})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN), c.parameters.get(ParameterName.REPLACEMENT))));

        functionBuilderMap.put(FunctionSignature.SUBSTRING_STRING, c ->
                c.builder.pattern("SUBSTRING({0}, CAST ({1} AS INTEGER), CAST({2} AS INTEGER))")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.POSITION), c.parameters.get(ParameterName.LENGTH))));

        functionBuilderMap.put(FunctionSignature.ADD_DATE, c ->
                c.builder.pattern("TIMESTAMPADD(SQL_TSI_DAY, {1}, {0})")
                        .parameters(List.of(c.parameters.get(ParameterName.DATE), c.parameters.get(ParameterName.ADDITION))));

        functionBuilderMap.put(FunctionSignature.DIFFERENCE_DATE, c ->
                c.builder.pattern("TIMESTAMPDIFF(SQL_TSI_DAY, {1}, {0})")
                        .parameters(List.of(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START))));

        functionBuilderMap.put(FunctionSignature.ADD_TIMESTAMP, c ->
                c.builder.pattern("TIMESTAMPADD(SQL_TSI_MILLI_SECOND, MOD({1}, 1000), TIMESTAMPADD(SQL_TSI_SECOND, {1} / 1000, {0}))")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP), c.parameters.get(ParameterName.ADDITION))));

        functionBuilderMap.put(FunctionSignature.ADD_TIME, c ->
                c.builder.pattern("TIMESTAMPADD(SQL_TSI_MILLI_SECOND, MOD({1}, 1000), TIMESTAMPADD(SQL_TSI_SECOND, {1} / 1000, CAST({0} AS TIMESTAMP)))")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME), c.parameters.get(ParameterName.ADDITION))));

        functionBuilderMap.put(FunctionSignature.DIFFERENCE_TIMESTAMP, c ->
                c.builder.pattern("TIMESTAMPDIFF(SQL_TSI_MILLI_SECOND, {1}, {0})")
                        .parameters(List.of(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START))));

        functionBuilderMap.put(FunctionSignature.DIFFERENCE_TIME, c ->
                c.builder.pattern("TIMESTAMPDIFF(SQL_TSI_MILLI_SECOND, CAST({1} AS TIMESTAMP), CAST({0} AS TIMESTAMP))")
                        .parameters(List.of(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START))));

        functionBuilderMap.put(FunctionSignature.IS_UNDEFINED_ATTRIBUTE, c ->
                c.builder.pattern("({0} IS NULL)")
                        .parameters(List.of(c.parameters.get(ParameterName.ATTRIBUTE))));

        functionBuilderMap.put(FunctionSignature.IS_UNDEFINED_OBJECT, c ->
                c.builder.pattern("({0} IS NULL)")
                        .parameters(List.of(c.parameters.get(ParameterName.RELATION))));

        functionBuilderMap.put(FunctionSignature.INSTANCE_OF, c ->
                c.builder.pattern("EXISTS (SELECT 1 FROM {1} WHERE " + StatementExecutor.ID_COLUMN_NAME + " = {0})")
                        .parameters(List.of(c.parameters.get(ParameterName.INSTANCE), c.parameters.get(ParameterName.TYPE))));

        functionBuilderMap.put(FunctionSignature.TYPE_OF, c ->
                c.builder.pattern("EXISTS (SELECT 1 FROM {1} WHERE " + StatementExecutor.ID_COLUMN_NAME + " = {0} AND " + StatementExecutor.ENTITY_TYPE_COLUMN_NAME + " = {2})")
                        .parameters(List.of(c.parameters.get(ParameterName.INSTANCE), c.parameters.get(ParameterName.TYPE), RdbmsConstant.builder()
                                .parameter(rdbmsBuilder.getParameterMapper().createParameter(AsmUtils.getClassifierFQName(((RdbmsEntityTypeName) c.parameters.get(ParameterName.TYPE)).getType()), null))
                                .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                                .build())));

        functionBuilderMap.put(FunctionSignature.MEMBER_OF, c ->
                c.builder.pattern("({0} IN ({1}))")
                        .parameters(List.of(c.parameters.get(ParameterName.INSTANCE), c.parameters.get(ParameterName.COLLECTION))));

        functionBuilderMap.put(FunctionSignature.EXISTS, c ->
                c.builder.pattern("EXISTS ({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.COLLECTION))));

        functionBuilderMap.put(FunctionSignature.NOT_EXISTS, c ->
                c.builder.pattern("(NOT EXISTS ({0}))")
                        .parameters(List.of(c.parameters.get(ParameterName.COLLECTION))));

        functionBuilderMap.put(FunctionSignature.COUNT, c ->
                c.builder.pattern("COUNT(DISTINCT {0})")
                        .parameters(List.of(c.parameters.get(ParameterName.ITEM))));

        functionBuilderMap.put(FunctionSignature.SUM_INTEGER, c ->
                c.builder.pattern("SUM({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));
        functionBuilderMap.put(FunctionSignature.SUM_DECIMAL, functionBuilderMap.get(FunctionSignature.SUM_INTEGER));

        functionBuilderMap.put(FunctionSignature.MIN_INTEGER, c ->
                c.builder.pattern("MIN({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));
        functionBuilderMap.put(FunctionSignature.MIN_DECIMAL, functionBuilderMap.get(FunctionSignature.MIN_INTEGER));

        functionBuilderMap.put(FunctionSignature.MIN_STRING, c ->
                c.builder.pattern("MIN({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING))));

        functionBuilderMap.put(FunctionSignature.MIN_DATE, c ->
                c.builder.pattern("MIN({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.DATE))));

        functionBuilderMap.put(FunctionSignature.MIN_TIMESTAMP, c ->
                c.builder.pattern("MIN({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.MIN_TIME, c ->
                c.builder.pattern("MIN({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        functionBuilderMap.put(FunctionSignature.MAX_INTEGER, c ->
                c.builder.pattern("MAX({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));
        functionBuilderMap.put(FunctionSignature.MAX_DECIMAL, functionBuilderMap.get(FunctionSignature.MAX_INTEGER));

        functionBuilderMap.put(FunctionSignature.MAX_STRING, c ->
                c.builder.pattern("MAX({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING))));

        functionBuilderMap.put(FunctionSignature.MAX_DATE, c ->
                c.builder.pattern("MAX({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.DATE))));

        functionBuilderMap.put(FunctionSignature.MAX_TIMESTAMP, c ->
                c.builder.pattern("MAX({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.MAX_TIME, c ->
                c.builder.pattern("MAX({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        functionBuilderMap.put(FunctionSignature.AVG_DECIMAL, c ->
                c.builder.pattern("AVG({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));

        functionBuilderMap.put(FunctionSignature.AVG_DATE, c ->
                c.builder.pattern("AVG({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.DATE))));

        functionBuilderMap.put(FunctionSignature.AVG_TIMESTAMP, c ->
                c.builder.pattern("AVG({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.AVG_TIME, c ->
                c.builder.pattern("AVG({0})")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        functionBuilderMap.put(FunctionSignature.CASE_WHEN, c ->
                c.builder.pattern("CASE WHEN {0} THEN {1} ELSE {2} END")
                        .parameters(List.of(c.parameters.get(ParameterName.CONDITION), c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        functionBuilderMap.put(FunctionSignature.UNDEFINED, c ->
                c.builder.pattern("NULL"));

        functionBuilderMap.put(FunctionSignature.YEARS_OF_DATE, c ->
                c.builder.pattern("CAST(EXTRACT(YEAR from CAST({0} AS DATE)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.DATE))));

        functionBuilderMap.put(FunctionSignature.MONTHS_OF_DATE, c ->
                c.builder.pattern("CAST(EXTRACT(MONTH from CAST({0} AS DATE)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.DATE))));

        functionBuilderMap.put(FunctionSignature.DAYS_OF_DATE, c ->
                c.builder.pattern("CAST(EXTRACT(DAY from CAST({0} AS DATE)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.DATE))));

        functionBuilderMap.put(FunctionSignature.YEARS_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(YEAR from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.MONTHS_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(MONTH from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.DAYS_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(DAY from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.HOURS_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(HOUR from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.MINUTES_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(MINUTE from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.SECONDS_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.MILLISECONDS_OF_TIMESTAMP, c ->
                c.builder.pattern("MOD(CAST(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) * 1000 AS INTEGER), 1000)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.HOURS_OF_TIME, c ->
                c.builder.pattern("CAST(EXTRACT(HOUR from CAST({0} AS TIME)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        functionBuilderMap.put(FunctionSignature.MINUTES_OF_TIME, c ->
                c.builder.pattern("CAST(EXTRACT(MINUTE from CAST({0} AS TIME)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        functionBuilderMap.put(FunctionSignature.SECONDS_OF_TIME, c ->
                c.builder.pattern("CAST(EXTRACT(SECOND from CAST({0} AS TIME)) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        functionBuilderMap.put(FunctionSignature.MILLISECONDS_OF_TIME, c ->
                c.builder.pattern("MOD(CAST(EXTRACT(SECOND from CAST({0} AS TIME)) * 1000 AS INTEGER), 1000)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        functionBuilderMap.put(FunctionSignature.TO_DATE, c ->
                c.builder.pattern("CAST(TO_DATE(CAST({0} AS INTEGER) || ''-'' || CAST({1} AS  INTEGER) || ''-'' || CAST({2} AS INTEGER), ''YYYY-MM-DD'') AS DATE)")
                        .parameters(List.of(
                                c.parameters.get(ParameterName.YEAR),
                                c.parameters.get(ParameterName.MONTH),
                                c.parameters.get(ParameterName.DAY)
                        )));

        functionBuilderMap.put(FunctionSignature.DATE_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(TO_DATE(" +
                                  "CAST(EXTRACT(YEAR from CAST({0} AS TIMESTAMP)) AS INTEGER) || ''-'' || " +
                                  "CAST(EXTRACT(MONTH from CAST({0} AS TIMESTAMP)) AS INTEGER) || ''-'' || " +
                                  "CAST(EXTRACT(DAY from CAST({0} AS TIMESTAMP)) AS INTEGER), " +
                                  "''YYYY-MM-DD'') AS DATE)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        functionBuilderMap.put(FunctionSignature.TO_TIMESTAMP, c ->
                c.builder.pattern("CAST(TO_TIMESTAMP(CAST({0} AS INTEGER) || ''-'' || CAST({1} AS INTEGER) || ''-'' || CAST({2} AS INTEGER) || '' '' || CAST({3} AS INTEGER) || '':'' || CAST({4} AS INTEGER) || '':'' || CAST({5} AS DECIMAL), ''YYYY-MM-DD HH24:MI:SS'') AS TIMESTAMP)")
                        .parameters(List.of(
                                c.parameters.get(ParameterName.YEAR),
                                c.parameters.get(ParameterName.MONTH),
                                c.parameters.get(ParameterName.DAY),
                                c.parameters.get(ParameterName.HOUR),
                                c.parameters.get(ParameterName.MINUTE),
                                c.parameters.get(ParameterName.SECOND)
                        )));

        functionBuilderMap.put(FunctionSignature.TO_TIME, c ->
                c.builder.pattern("CAST(CAST({0} AS INTEGER) || '':'' || CAST({1} AS INTEGER) || '':'' || CAST({2} AS INTEGER) AS TIME)")
                        .parameters(List.of(
                                c.parameters.get(ParameterName.HOUR),
                                c.parameters.get(ParameterName.MINUTE),
                                c.parameters.get(ParameterName.SECOND)
                        )));

        functionBuilderMap.put(FunctionSignature.TIME_OF_TIMESTAMP, c ->
                c.builder.pattern("CAST(" +
                                  "CAST(EXTRACT(HOUR from CAST({0} AS TIMESTAMP)) AS INTEGER) || '':'' || " +
                                  "CAST(EXTRACT(MINUTE from CAST({0} AS TIMESTAMP)) AS INTEGER) || '':'' || " +
                                  "CAST(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) AS INTEGER) " +
                                  "AS TIME)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));
    }

    @Override
    public Stream<? extends RdbmsField> map(final Function function, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final EMap<ParameterName, RdbmsField> parameters = ECollections.asEMap(function.getParameters().stream()
                .collect(Collectors.<FunctionParameter, ParameterName, RdbmsField>toMap(
                        FunctionParameter::getParameterName,
                        e -> rdbmsBuilder.mapFeatureToRdbms(e.getParameterValue(), ancestors, parentIdFilterQuery, queryParameters).findAny().
                                orElseThrow(() -> new IllegalStateException("Rdbms field not found for parameter: " + e.getParameterName())))));

        return getTargets(function).map(t -> {
            RdbmsFunction.RdbmsFunctionBuilder builder = RdbmsFunction.builder()
                    .target(t.getTarget())
                    .targetAttribute(t.getTargetAttribute())
                    .alias(t.getAlias());

            java.util.function.Function<FunctionContext, RdbmsFunction.RdbmsFunctionBuilder> func = functionBuilderMap.get(function.getSignature());
            if (func == null) {
                throw new UnsupportedOperationException("Unsupported function: " + function.getSignature());
            }

            return func.apply(FunctionContext.builder()
                    .function(function)
                    .parameters(parameters)
                    .builder(builder)
                    .build()).build();


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
