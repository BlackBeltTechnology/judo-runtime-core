package hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.mappers;

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

import hu.blackbelt.judo.meta.query.FunctionSignature;
import hu.blackbelt.judo.meta.query.ParameterName;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.FunctionMapper;
import lombok.Builder;
import lombok.NonNull;

import java.util.List;

public class HsqldbFunctionMapper<ID> extends FunctionMapper<ID> {

    @SuppressWarnings("unchecked")
    @Builder
    public HsqldbFunctionMapper(@NonNull RdbmsBuilder<ID> rdbmsBuilder) {
        super(rdbmsBuilder);

        getFunctionBuilderMap().put(FunctionSignature.LIKE, c ->
                c.builder.pattern("(CASE " +
                                      "WHEN ({0} LIKE {1}) THEN (1 = 1) " +
                                      "WHEN ({0} IS NULL) THEN NULL " + // {1} is string constant => cannot be null (here)
                                      "ELSE (0 = 1) " +
                                  "END)")
                         .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN))));

        getFunctionBuilderMap().put(FunctionSignature.ILIKE, c ->
                c.builder.pattern("(CASE " +
                                      "WHEN (LOWER({0}) LIKE {1}) THEN (1 = 1) " + // {1} is lowercase after feature conversion
                                      "WHEN ({0} IS NULL) THEN NULL " + // {1} is string constant => cannot be null (here)
                                      "ELSE (0 = 1) " +
                                  "END)")
                         .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN))));

        getFunctionBuilderMap().put(FunctionSignature.HOURS_OF_TIME, c ->
                c.builder.pattern("CAST(EXTRACT(HOUR from {0}) AS INTEGER)")
                         .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        getFunctionBuilderMap().put(FunctionSignature.MINUTES_OF_TIME, c ->
                c.builder.pattern("CAST(EXTRACT(MINUTE from {0}) AS INTEGER)")
                         .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        getFunctionBuilderMap().put(FunctionSignature.SECONDS_OF_TIME, c ->
                c.builder.pattern("FLOOR(EXTRACT(SECOND from {0}))")
                         .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        getFunctionBuilderMap().put(FunctionSignature.TIME_OF_TIMESTAMP, c ->
                c.builder.pattern("TO_TIMESTAMP(EXTRACT(HOUR FROM {0}) || '':'' || EXTRACT(MINUTE FROM {0}) || '':'' || EXTRACT(SECOND FROM {0}), ''HH24:MI:SS.FF'')")
                         .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_AS_MILLISECONDS, c ->
                c.builder.pattern("(UNIX_MILLIS(CAST({0} AS TIMESTAMP)))")
                         .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        getFunctionBuilderMap().put(FunctionSignature.TIME_AS_MILLISECONDS, c ->
                c.builder.pattern("(UNIX_MILLIS(CAST({0} AS TIMESTAMP)))")
                         .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_FROM_MILLISECONDS, c ->
                c.builder.pattern("DATEADD(MILLISECOND, MOD({0}, 1000), TIMESTAMP({0} / 1000.0))")
                         .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));
        getFunctionBuilderMap().put(FunctionSignature.TIME_FROM_MILLISECONDS, getFunctionBuilderMap().get(FunctionSignature.TIMESTAMP_FROM_MILLISECONDS));

        getFunctionBuilderMap().put(FunctionSignature.MILLISECONDS_OF_TIME, c ->
                c.builder.pattern("MOD(FLOOR(EXTRACT(SECOND from {0}) * 1000), 1000)") // TODO: check if this is working for postgresql as well
                         .parameters(List.of(c.parameters.get(ParameterName.TIME))));

        getFunctionBuilderMap().put(FunctionSignature.TO_TIME, c ->
                c.builder.pattern("TO_TIMESTAMP({0} || '':'' || {1} || '':'' || {2} || ''.'' || MOD({3}, 1000), ''HH24:MI:SS.FF'')")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.HOUR),
                                 c.parameters.get(ParameterName.MINUTE),
                                 c.parameters.get(ParameterName.SECOND),
                                 c.parameters.get(ParameterName.MILLISECOND)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.DAY_OF_WEEK_OF_DATE, c -> {
            String sqlDayOfWeek = "CAST(EXTRACT(DAY_OF_WEEK FROM {0}) AS INTEGER)";
            return c.builder.pattern("(CASE WHEN " + sqlDayOfWeek + " = 1 THEN 7 ELSE (" + sqlDayOfWeek + " - 1) END)")
                            .parameters(List.of(c.parameters.get(ParameterName.DATE)));
        });

        getFunctionBuilderMap().put(FunctionSignature.DAY_OF_YEAR_DATE, c ->
                c.builder.pattern("CAST(EXTRACT(DAY_OF_YEAR FROM {0}) AS INTEGER)")
                         .parameters(List.of(c.parameters.get(ParameterName.DATE))));

        getFunctionBuilderMap().put(FunctionSignature.TIME_TO_STRING, c -> {
            String time = "TO_CHAR({0}, ''HH24:MI<second>'') ";
            String fractionalPartRequired = "FLOOR(EXTRACT(SECOND FROM {0})) < EXTRACT(SECOND FROM {0}) ";
            String secondPartRequired = "EXTRACT(SECOND FROM {0}) > 0 ";

            return c.builder.pattern("(CASE " +
                                         "WHEN " + fractionalPartRequired + " THEN " + time.replace("<second>", ":SS.FF") +
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
                                         "WHEN " + fractionalPartRequired + " THEN " + timestamp.replace("<second>", ":SS.FF") +
                                         "WHEN " + secondPartRequired + " THEN " + timestamp.replace("<second>", ":SS") +
                                         "ELSE " + timestamp.replace("<second>", "") +
                                     "END)")
                            .parameters(List.of(c.parameters.get(ParameterName.PRIMITIVE)));
        });

        getFunctionBuilderMap().put(FunctionSignature.ADD_TIME, c ->
                c.builder.pattern("TIMESTAMPADD(SQL_TSI_MILLI_SECOND, MOD({1}, 1000), TIMESTAMPADD(SQL_TSI_SECOND, {1} / 1000, {0}))")
                         .parameters(List.of(c.parameters.get(ParameterName.TIME), c.parameters.get(ParameterName.ADDITION))));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_YEARS, c ->
                c.builder.pattern("DATEADD(YEAR, {0}, {1})")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.NUMBER),
                                 c.parameters.get(ParameterName.TIMESTAMP)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_MONTHS, c ->
                c.builder.pattern("DATEADD(MONTH, {0}, {1})")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.NUMBER),
                                 c.parameters.get(ParameterName.TIMESTAMP)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_DAYS, c ->
                c.builder.pattern("DATEADD(DAY, {0}, {1})")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.NUMBER),
                                 c.parameters.get(ParameterName.TIMESTAMP)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_HOURS, c ->
                c.builder.pattern("DATEADD(HOUR, {0}, {1})")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.NUMBER),
                                 c.parameters.get(ParameterName.TIMESTAMP)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_MINUTES, c ->
                c.builder.pattern("DATEADD(MINUTE, {0}, {1})")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.NUMBER),
                                 c.parameters.get(ParameterName.TIMESTAMP)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_SECONDS, c ->
                c.builder.pattern("DATEADD(SECOND, {0}, {1})")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.NUMBER),
                                 c.parameters.get(ParameterName.TIMESTAMP)
                         )));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_PLUS_MILLISECONDS, c ->
                c.builder.pattern("DATEADD(MILLISECOND, {0}, {1})")
                         .parameters(List.of(
                                 c.parameters.get(ParameterName.NUMBER),
                                 c.parameters.get(ParameterName.TIMESTAMP)
                         )));

    }
}
