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
                                   "WHEN {2} THEN (CASE " +
                                                  "WHEN ({0} IS NULL or {1} IS NULL) THEN NULL " +
                                                  "WHEN (LOWER({0}) LIKE LOWER({1})) THEN (1 = 1) " +
                                                  "ELSE (1 = 0) END) " +
                                   "ELSE (CASE " +
                                         "WHEN ({0} IS NULL or {1} IS NULL) THEN NULL " +
                                         "WHEN ({0} LIKE {1}) THEN (1 = 1) " +
                                         "ELSE (1 = 0) END) " +
                                   "END)")
                         .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN), c.parameters.get(ParameterName.CONDITION))));

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
            return c.builder.pattern(String.format("(CASE %1$s WHEN =1 THEN 7 ELSE (%1$s - 1) END)",
                                                   "CAST(EXTRACT(DAY_OF_WEEK FROM {0}) AS INTEGER)"))
                            .parameters(List.of(c.parameters.get(ParameterName.DATE)));
        });

        getFunctionBuilderMap().put(FunctionSignature.DAY_OF_YEAR_DATE, c ->
                c.builder.pattern("CAST(EXTRACT(DAY_OF_YEAR FROM {0}) AS INTEGER)")
                         .parameters(List.of(c.parameters.get(ParameterName.DATE))));

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
