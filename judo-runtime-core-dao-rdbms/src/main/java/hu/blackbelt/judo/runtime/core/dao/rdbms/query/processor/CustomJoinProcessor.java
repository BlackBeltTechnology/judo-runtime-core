package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

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
import hu.blackbelt.judo.meta.query.CustomJoin;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsCustomJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Builder
@Slf4j
public class CustomJoinProcessor {
    @NonNull
    private final RdbmsResolver rdbmsResolver;

    @NonNull
    private final AsmUtils asmUtils;

    public List<RdbmsJoin> process(CustomJoin join, RdbmsBuilderContext builderContext) {
        final EMap<Node, EList<EClass>> ancestors = builderContext.getAncestors();
        final RdbmsBuilder rdbmsBuilder = builderContext.getRdbmsBuilder();

        if (log.isTraceEnabled()) {
            log.trace("Node:       " + join);
            log.trace(builderContext.toString());
        }

        final List<RdbmsJoin> joins = new ArrayList<>();
        final String sql;
        if (join.getNavigationSql().indexOf('`') != -1) {
            sql = resolveRdbmsNames(join.getNavigationSql());
        } else {
            sql = join.getNavigationSql();
        }

        joins.add(RdbmsCustomJoin.builder()
                .sql(sql)
                .sourceIdSetParameterName(join.getSourceIdSetParameter())
                .alias(join.getAlias())
                .columnName(join.getSourceIdParameter())
                .partnerTable(join.getPartner())
                .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                .outer(true)
                .build());

        if (ancestors.containsKey(join)) {
            rdbmsBuilder.addAncestorJoins(joins, join, builderContext);
        }
        return joins;
    }

    private String resolveRdbmsNames(final String sql) {
        final StringBuilder result = new StringBuilder();
        final StringCharacterIterator it = new StringCharacterIterator(sql);

        boolean resolving = false;
        StringBuilder fqNameBuilder = null;
        for (char ch = it.first(); ch != CharacterIterator.DONE; ch = it.next()) {
            if (ch == '`' && resolving) {
                final String fqName = fqNameBuilder.toString();

                final Optional<EAttribute> attribute = asmUtils.resolveAttribute(fqName);
                final Optional<EReference> reference = attribute.isPresent() ? Optional.empty() : asmUtils.resolveReference(fqName);
                final Optional<EClass> type = attribute.isPresent() || reference.isPresent() ? Optional.empty() : asmUtils.resolve(fqName).filter(c -> c instanceof EClass).map(c -> (EClass) c);

                // TODO - support resolving junction table names
                if (attribute.isPresent()) {
                    result.append(rdbmsResolver.rdbmsField(attribute.get()).getSqlName());
                } else if (reference.isPresent()) {
                    result.append(rdbmsResolver.rdbmsField(reference.get()).getSqlName());
                } else if (type.isPresent()) {
                    result.append(rdbmsResolver.rdbmsTable(type.get()).getSqlName());
                } else {
                    throw new IllegalStateException("Unable to resolve ASM element name: " + fqName);
                }

                resolving = false;
            } else if (ch == '`' && !resolving) {
                fqNameBuilder = new StringBuilder();
                resolving = true;
            } else if (resolving) {
                fqNameBuilder.append(ch);
            } else {
                result.append(ch);
            }
        }

        if (resolving) {
            log.error("SQL syntax is invalid (terminated while resolving RDBMS name): {}", sql);
            throw new IllegalArgumentException("Invalid custom SQL");
        }

        return result.toString();
    }

}
