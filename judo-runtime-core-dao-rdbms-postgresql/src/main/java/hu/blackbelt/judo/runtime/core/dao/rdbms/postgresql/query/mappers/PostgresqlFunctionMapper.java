package hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.query.mappers;

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

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.FunctionMapper;
import lombok.Builder;
import lombok.NonNull;

import java.util.List;

public class PostgresqlFunctionMapper<ID> extends FunctionMapper<ID> {

    @SuppressWarnings("unchecked")
    @Builder
    public PostgresqlFunctionMapper(@NonNull RdbmsBuilder<ID> rdbmsBuilder) {
        super(rdbmsBuilder);

        getFunctionBuilderMap().put(FunctionSignature.MODULO_INTEGER, c ->
                c.builder.pattern("({0} % {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        getFunctionBuilderMap().put(FunctionSignature.MODULO_DECIMAL, c ->
                c.builder.pattern("({0} % {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.LEFT), c.parameters.get(ParameterName.RIGHT))));

        getFunctionBuilderMap().put(FunctionSignature.INTEGER_TO_STRING, c ->
                c.builder.pattern("CAST({0} AS TEXT)")
                        .parameters(List.of(c.parameters.get(ParameterName.PRIMITIVE))));
        getFunctionBuilderMap().put(FunctionSignature.DECIMAL_TO_STRING, getFunctionBuilderMap().get(FunctionSignature.INTEGER_TO_STRING));
        getFunctionBuilderMap().put(FunctionSignature.DATE_TO_STRING, getFunctionBuilderMap().get(FunctionSignature.INTEGER_TO_STRING));
        getFunctionBuilderMap().put(FunctionSignature.LOGICAL_TO_STRING, c ->
                c.builder.pattern("LOWER(CAST({0} AS TEXT))")
                         .parameters(List.of(c.parameters.get(ParameterName.PRIMITIVE))));
        getFunctionBuilderMap().put(FunctionSignature.ENUM_TO_STRING, getFunctionBuilderMap().get(FunctionSignature.INTEGER_TO_STRING));
        getFunctionBuilderMap().put(FunctionSignature.CUSTOM_TO_STRING, getFunctionBuilderMap().get(FunctionSignature.INTEGER_TO_STRING));

        getFunctionBuilderMap().put(FunctionSignature.TIME_TO_STRING, c -> {
            String time = "TO_CHAR({0}, ''HH24:MI<second>'') ";
            String fractionalPartRequired = "FLOOR(EXTRACT(SECOND FROM {0})) < EXTRACT(SECOND FROM {0}) ";
            String secondPartRequired = "EXTRACT(SECOND FROM {0}) > 0 ";

            return c.builder.pattern("(CASE " +
                                         "WHEN " + fractionalPartRequired + " THEN " + time.replace("<second>", ":SS.FF3") +
                                         "WHEN " + secondPartRequired + " THEN " + time.replace("<second>", ":SS") +
                                         "ELSE " + time.replace("<second>", "") +
                                     "END)")
                            .parameters(List.of(c.parameters.get(ParameterName.PRIMITIVE)));
        });

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_TO_STRING, c -> {
            String timestamp = "REPLACE(TO_CHAR({0}, ''YYYY-MM-DD HH24:MI<second>''), '' '', ''T'') ";
            String fractionalPartRequired = "FLOOR(EXTRACT(SECOND FROM {0})) < EXTRACT(SECOND FROM {0}) ";
            String secondPartRequired = "EXTRACT(SECOND FROM {0}) > 0 ";

            return c.builder.pattern("(CASE " +
                                         "WHEN " + fractionalPartRequired + " THEN " + timestamp.replace("<second>", ":SS.FF3") +
                                         "WHEN " + secondPartRequired + " THEN " + timestamp.replace("<second>", ":SS") +
                                         "ELSE " + timestamp.replace("<second>", "") +
                                     "END)")
                            .parameters(List.of(c.parameters.get(ParameterName.PRIMITIVE)));
        });

        getFunctionBuilderMap().put(FunctionSignature.MATCHES_STRING, c ->
                c.builder.pattern("({0} ~ {1})")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN))));

        getFunctionBuilderMap().put(FunctionSignature.LEFT_PAD, c ->
                c.builder.pattern("LPAD({0}, {1}, {2})")
                         .parameters(List.of(c.parameters.get(ParameterName.STRING),
                                             c.parameters.get(ParameterName.LENGTH),
                                             c.parameters.get(ParameterName.REPLACEMENT))));

        getFunctionBuilderMap().put(FunctionSignature.RIGHT_PAD, c ->
                c.builder.pattern("RPAD({0}, {1}, {2})")
                         .parameters(List.of(c.parameters.get(ParameterName.STRING),
                                             c.parameters.get(ParameterName.LENGTH),
                                             c.parameters.get(ParameterName.REPLACEMENT))));

        getFunctionBuilderMap().put(FunctionSignature.ADD_DATE, c ->
                c.builder.pattern("({0} + CAST({1} || '' days'' AS INTERVAL))")
                        .parameters(List.of(c.parameters.get(ParameterName.DATE), c.parameters.get(ParameterName.ADDITION))));

        getFunctionBuilderMap().put(FunctionSignature.DIFFERENCE_DATE, c ->
                c.builder.pattern("(CAST({0} AS DATE) - CAST({1} AS DATE))")
                        .parameters(List.of(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START))));

        getFunctionBuilderMap().put(FunctionSignature.ADD_TIMESTAMP, c ->
                c.builder.pattern("({0} + CAST({1} / 1000 || '' seconds'' AS INTERVAL))")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP), c.parameters.get(ParameterName.ADDITION))));

        getFunctionBuilderMap().put(FunctionSignature.ADD_TIME, c ->
                c.builder.pattern("({0} + CAST({1} / 1000 || '' seconds'' AS INTERVAL))")
                        .parameters(List.of(c.parameters.get(ParameterName.TIME), c.parameters.get(ParameterName.ADDITION))));

        getFunctionBuilderMap().put(FunctionSignature.DIFFERENCE_TIMESTAMP, c ->
                c.builder.pattern("(EXTRACT(EPOCH FROM {0} - {1}) * 1000)")
                        .parameters(List.of(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START))));

        getFunctionBuilderMap().put(FunctionSignature.DIFFERENCE_TIME, c ->
                c.builder.pattern("(EXTRACT(EPOCH FROM {0} - {1}) * 1000)")
                        .parameters(List.of(c.parameters.get(ParameterName.END), c.parameters.get(ParameterName.START))));

        getFunctionBuilderMap().put(FunctionSignature.MILLISECONDS_OF_TIMESTAMP, c ->
                c.builder.pattern("(FLOOR(EXTRACT(SECOND from CAST({0} AS TIMESTAMP)) * 1000) % 1000)")
                        .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_AS_MILLISECONDS, c ->
                c.builder.pattern("(EXTRACT(EPOCH FROM (CAST({0} AS TIMESTAMP))) * 1000)")
                         .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_FROM_MILLISECONDS, c ->
                c.builder.pattern("CAST(TO_TIMESTAMP({0}::double precision / 1000) AT TIME ZONE INTERVAL ''+0:00'' HOUR TO MINUTE AS TIMESTAMP)")
                         .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));

        getFunctionBuilderMap().put(FunctionSignature.TIME_FROM_MILLISECONDS, c ->
                c.builder.pattern("CAST(TO_TIMESTAMP({0}::double precision / 1000) AT TIME ZONE INTERVAL ''+0:00'' HOUR TO MINUTE AS TIME)")
                         .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));

        getFunctionBuilderMap().put(FunctionSignature.TIME_AS_MILLISECONDS, c ->
                c.builder.pattern("(EXTRACT(EPOCH FROM (CAST({0} AS TIME))) * 1000)")
                         .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        getFunctionBuilderMap().put(FunctionSignature.DAY_OF_WEEK_OF_DATE, c -> {
            String sqlDayOfWeek = "CAST(EXTRACT(DOW FROM {0}) AS INTEGER)";
            return c.builder.pattern("(CASE WHEN " + sqlDayOfWeek + " = 0 THEN 7 ELSE " + sqlDayOfWeek + " END)")
                            .parameters(List.of(c.parameters.get(ParameterName.DATE)));
        });

        getFunctionBuilderMap().put(FunctionSignature.DAY_OF_YEAR_DATE, c ->
                c.builder.pattern("CAST(EXTRACT(DOY FROM {0}) AS INTEGER)")
                        .parameters(List.of(c.parameters.get(ParameterName.DATE))));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_YEARS, c ->
                c.builder.pattern("({0} + CAST({1} || '' years'' AS INTERVAL))")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.TIMESTAMP),
                                 c.parameters.get(ParameterName.NUMBER)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_MONTHS, c ->
                c.builder.pattern("({0} + CAST({1} || '' months'' AS INTERVAL))")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.TIMESTAMP),
                                 c.parameters.get(ParameterName.NUMBER)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_DAYS, c ->
                c.builder.pattern("({0} + CAST({1} || '' days'' AS INTERVAL))")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.TIMESTAMP),
                                 c.parameters.get(ParameterName.NUMBER)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_HOURS, c ->
                c.builder.pattern("({0} + CAST({1} || '' hours'' AS INTERVAL))")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.TIMESTAMP),
                                 c.parameters.get(ParameterName.NUMBER)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_MINUTES, c ->
                c.builder.pattern("({0} + CAST({1} || '' minutes'' AS INTERVAL))")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.TIMESTAMP),
                                 c.parameters.get(ParameterName.NUMBER)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_SECONDS, c ->
                c.builder.pattern("({0} + CAST({1} || '' seconds'' AS INTERVAL))")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.TIMESTAMP),
                                 c.parameters.get(ParameterName.NUMBER)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_MILLISECONDS, c ->
                c.builder.pattern("({0} + CAST(({1}::double precision / 1000) || '' seconds'' AS INTERVAL))")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.TIMESTAMP),
                                 c.parameters.get(ParameterName.NUMBER)
                         )));
    }
}
