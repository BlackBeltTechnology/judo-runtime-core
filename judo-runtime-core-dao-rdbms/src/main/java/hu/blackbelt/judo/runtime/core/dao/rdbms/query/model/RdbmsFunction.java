package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

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

import lombok.Singular;
import lombok.experimental.SuperBuilder;

import java.text.MessageFormat;
import java.util.List;

/**
 * RDBMS function definition.
 */
@SuperBuilder
public class RdbmsFunction extends RdbmsField {

    private String pattern;

    @Singular
    private List<RdbmsField> parameters;

    @Override
    public String toSql(SqlConverterContext converterContext) {
        final boolean includeAlias = converterContext.isIncludeAlias();

        final String sql = cast(MessageFormat.format(pattern, parameters.stream()
                .map(p -> p.toSql(converterContext.toBuilder()
                        .includeAlias(false)
                        .build())).toArray()),
                null, targetAttribute);
        return getWithAlias(sql, includeAlias);
    }
}
