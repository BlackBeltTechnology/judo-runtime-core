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

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.Target;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.types.RdbmsDecimalType;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EAnnotation;
import org.eclipse.emf.ecore.EAttribute;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.Optional;

@SuperBuilder
@NoArgsConstructor
public abstract class RdbmsField {

    protected String alias;

    protected Target target;

    @Getter
    protected EAttribute targetAttribute;

    /**
     * Convert field definition to SQL format.
     *
     * @return SQL string
     */
    public abstract String toSql(String prefix, boolean includeAlias, Coercer coercer, MapSqlParameterSource sqlParameters, EMap<Node, String> prefixes);

    protected String getWithAlias(final String sql, final boolean includeAlias) {
        return sql + (includeAlias && alias != null ? " AS " + getRdbmsAlias() : "");
    }

    public String getRdbmsAlias() {
        return (target != null ? RdbmsAliasUtil.getTargetColumnAlias(target, alias) : alias);
    }

    public static DomainConstraints getDomainConstraints(final EAttribute attribute) {
        final Optional<EAnnotation> constraints = AsmUtils.getExtensionAnnotationByName(attribute, "constraints", false);

        if (constraints.isPresent()) {
            final EMap<String, String> details = constraints.get().getDetails();
            return DomainConstraints.builder()
                    .precision(details.containsKey("precision") ? Integer.parseInt(details.get("precision")) : null)
                    .scale(details.containsKey("scale") ? Integer.parseInt(details.get("scale")) : null)
                    .maxLength(details.containsKey("maxLength") ? Integer.parseInt(details.get("maxLength")) : null)
                    .build();
        } else {
            return null;
        }
    }

    private String convertDomainConstraintsToSqlType(final DomainConstraints domainConstraints) {
        final String sqlType;
        if (domainConstraints == null) {
            sqlType = "";
        } else if (domainConstraints.getPrecision() != null) {
            sqlType = new RdbmsDecimalType(domainConstraints.getPrecision() + domainConstraints.getScale(), domainConstraints.getScale()).toSql();
        } else if (domainConstraints.getMaxLength() != null) {
            sqlType = "VARCHAR(" + domainConstraints.getMaxLength() + ")";
        } else {
            sqlType = "";
        }
        return sqlType;
    }

    protected String cast(final String sql, final String typeName, final EAttribute targetAttribute) {
        final String sqlType;
        final DomainConstraints domainConstraints;
        if (targetAttribute != null) {
            domainConstraints = getDomainConstraints(targetAttribute);
            sqlType = convertDomainConstraintsToSqlType(domainConstraints);
        } else {
            domainConstraints = null;
            sqlType = "";
        }
        if (typeName != null && !typeName.isBlank()) {
            return "CAST(" + sql + " AS " + typeName + ")";
        } else if (sqlType != null && !sqlType.isBlank()) {
            if (domainConstraints != null && domainConstraints.getScale() != null) {
                String defaultType = new RdbmsDecimalType().toSql();
                if (domainConstraints.getScale() == 0) {
                    return String.format("CAST(FLOOR(CAST(%s AS %s)) AS %s)", sql, defaultType, sqlType);
                } else {
                    String zeros = "0".repeat(domainConstraints.getScale());
                    return String.format("CAST(CAST(FLOOR(CAST(%s AS %s) * 1%s) AS %s) / 1%s AS %s)",
                                         sql, defaultType, zeros, defaultType, zeros, sqlType);
                }
            } else {
                return "CAST(" + sql + " AS " + sqlType + ")";
            }
        } else {
            return sql;
        }
    }

    @Getter
    @Builder
    public static class DomainConstraints {
        private final Integer precision;
        private final Integer scale;
        private final Integer maxLength;
    }

    @Override
    public String toString() {
        return "RdbmsField{" +
               "alias='" + alias + '\'' +
               '}';
    }

}
