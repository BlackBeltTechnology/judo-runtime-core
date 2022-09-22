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

        getFunctionBuilderMap().put(FunctionSignature.ILIKE, c ->
                c.builder.pattern("(LOWER({0}) LIKE LOWER({1}))")
                        .parameters(List.of(c.parameters.get(ParameterName.STRING), c.parameters.get(ParameterName.PATTERN))));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_AS_MILLISECONDS, c ->
                c.builder.pattern("(UNIX_MILLIS(CAST({0} AS TIMESTAMP)))")
                         .parameters(List.of(c.parameters.get(ParameterName.TIMESTAMP))));

        getFunctionBuilderMap().put(FunctionSignature.TIMESTAMP_FROM_MILLISECONDS, c ->
                c.builder.pattern("DATEADD(MILLISECOND, MOD({0}, 1000), TIMESTAMP({0} / 1000))")
                         .parameters(List.of(c.parameters.get(ParameterName.NUMBER))));

    }
}

