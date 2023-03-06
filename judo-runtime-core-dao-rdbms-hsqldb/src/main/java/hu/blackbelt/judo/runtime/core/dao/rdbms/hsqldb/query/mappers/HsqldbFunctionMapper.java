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

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_AS_MILLISECONDS, c ->
                c.builder.pattern("(UNIX_MILLIS(CAST({0} AS TIMESTAMP)))")
                         .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_FROM_MILLISECONDS, c ->
                c.builder.pattern("DATEADD(MILLISECOND, MOD({0}, 1000), TIMESTAMP({0} / 1000))")
                         .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));

        getFunctionBuilderMap().put(FunctionSignature.TIME_FROM_SECONDS, c ->
                c.builder.pattern("CAST(" +
                                  "CAST(EXTRACT(HOUR from TIMESTAMP({0})) AS INTEGER) || '':'' || " +
                                  "CAST(EXTRACT(MINUTE from TIMESTAMP({0})) AS INTEGER) || '':'' || " +
                                  "CAST(EXTRACT(SECOND from TIMESTAMP({0})) AS INTEGER) " +
                                  "AS TIME)")
                         .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));

        getFunctionBuilderMap().put(FunctionSignature.DAY_OF_WEEK_OF_DATE, c -> {
            String sqlDayOfWeek = "CAST(EXTRACT(DAY_OF_WEEK FROM {0}) AS INTEGER)";
            return c.builder.pattern("(CASE WHEN " + sqlDayOfWeek + " = 1 THEN 7 ELSE (" + sqlDayOfWeek + " - 1) END)")
                            .parameters(List.of(c.parameters.get(ParameterName.DATE)));
        });

        getFunctionBuilderMap().put(FunctionSignature.DAY_OF_YEAR_DATE, c ->
                c.builder.pattern("CAST(EXTRACT(DAY_OF_YEAR FROM {0}) AS INTEGER)")
                         .parameters(List.of(c.parameters.get(ParameterName.DATE))));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_TO_STRING, c -> {
            String year = "EXTRACT(YEAR FROM CAST({0} AS TIMESTAMP WITH TIME ZONE))";
            String month = "EXTRACT(MONTH FROM CAST({0} AS TIMESTAMP WITH TIME ZONE))";
            String monthPadded = "LPAD(" + month + ", 2, ''0'')";
            String day = "EXTRACT(DAY FROM CAST({0} AS TIMESTAMP WITH TIME ZONE))";
            String dayPadded = "LPAD(" + day + ", 2, ''0'')";

            String hour = "EXTRACT(HOUR FROM CAST({0} AS TIMESTAMP WITH TIME ZONE))";
            String hourPadded = "LPAD(" + hour + ", 2, ''0'')";
            String minute = "EXTRACT(MINUTE FROM CAST({0} AS TIMESTAMP WITH TIME ZONE))";
            String minutePadded = "LPAD(" + minute + ", 2, ''0'')";

            String milli = "MOD(FLOOR(EXTRACT(SECOND FROM CAST({0} AS TIMESTAMP WITH TIME ZONE)) * 1000), 1000)";
            String milliPadded = "LPAD(" + milli + ", 3, ''0'')";
            String second = "FLOOR(EXTRACT(SECOND FROM CAST({0} AS TIMESTAMP WITH TIME ZONE)))";
            String secondPadded = "LPAD(" + second + ", 2, ''0'')";
            // empty string concatenation actually concatenates a whitespace => slightly confusing CASE-WHEN as workaround
            String secondFormatted = "(CASE " +
                                         "WHEN " + milli + " > 0 THEN " + secondPadded + " || ''.'' || " + milliPadded + " " +
                                         "ELSE " + secondPadded +
                                     "END)";

            String timezoneHour = "EXTRACT(TIMEZONE_HOUR FROM CAST({0} AS TIMESTAMP WITH TIME ZONE))";
            String timezoneMinute = "EXTRACT(TIMEZONE_MINUTE FROM CAST({0} AS TIMESTAMP WITH TIME ZONE))";
            String timezoneSign = "(CASE " +
                                      "WHEN " + timezoneHour + " < 0 OR " + timezoneMinute + " < 0 THEN ''-'' " +
                                      "ELSE ''+'' " +
                                  "END)";
            String timezoneHourPadded = "LPAD(ABS(" + timezoneHour + "), 2, ''0'')";
            String timezoneMinutePadded = "LPAD(ABS(" + timezoneMinute + "), 2, ''0'')";

            String timezoneFormatted = "(CASE " +
                                           "WHEN " + timezoneHour + " = 0 AND " + timezoneMinute + " = 0 THEN ''Z'' " +
                                           "ELSE (" + timezoneSign + " || " + timezoneHourPadded + " || '':'' ||" + timezoneMinutePadded + ")" +
                                       "END)";

            return c.builder.pattern("(" +
                                         year + " || ''-'' || " + monthPadded + " || ''-'' || " + dayPadded + " || " +
                                         "''T'' || " +
                                         hourPadded + " || '':'' || " + minutePadded + " || '':'' || " + secondFormatted + " || " +
                                         timezoneFormatted +
                                     ")")
                            .parameters(List.of(c.parameters.get(ParameterName.PRIMITIVE)));
        });

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
