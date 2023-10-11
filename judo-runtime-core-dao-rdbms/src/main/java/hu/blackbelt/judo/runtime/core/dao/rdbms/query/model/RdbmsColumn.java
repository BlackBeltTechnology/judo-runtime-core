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

import hu.blackbelt.judo.meta.query.Node;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.text.MessageFormat;
import java.util.regex.Pattern;

/**
 * RDBMS column definition.
 */
@SuperBuilder
public class RdbmsColumn extends RdbmsField {

    private String pattern;
    private String partnerTablePrefix;
    private Node partnerTable;
    private String partnerTablePostfix;
    private String columnName;

    private boolean skipLastPrefix;

    @Getter
    private DomainConstraints sourceDomainConstraints;

    private static final String DEFAULT_PATTERN = "{0}.{1}";

    @Override
    public String toSql(SqlConverterContext context) {
        final String partnerTableName;
        if (partnerTable != null) {
            final String partnerTableNameWithPrefix;
            if (context.prefixes.containsKey(partnerTable)) {
                partnerTableNameWithPrefix = context.prefixes.get(partnerTable) + (partnerTablePrefix != null ? partnerTablePrefix : "") + partnerTable.getAlias() + (partnerTablePostfix != null ? partnerTablePostfix : "");
            } else {
                partnerTableNameWithPrefix = context.prefix + (partnerTablePrefix != null ? partnerTablePrefix : "") + partnerTable.getAlias() + (partnerTablePostfix != null ? partnerTablePostfix : "");
            }
            if (skipLastPrefix) {
                partnerTableName = partnerTableNameWithPrefix.replaceFirst("^" + Pattern.quote(context.prefix), "");
            } else {
                partnerTableName = partnerTableNameWithPrefix;
            }
        } else {
            partnerTableName = null;
        }

        final String sql = cast(MessageFormat.format(pattern != null ? pattern : DEFAULT_PATTERN, new Object[]{partnerTableName, columnName}), null, targetAttribute);
        return getWithAlias(sql, context.includeAlias);
    }

    @Override
    public String toString() {
        return "RdbmsColumn{" +
               "columnName='" + columnName + '\'' +
               ", alias='" + alias + '\'' +
               '}';
    }

}
