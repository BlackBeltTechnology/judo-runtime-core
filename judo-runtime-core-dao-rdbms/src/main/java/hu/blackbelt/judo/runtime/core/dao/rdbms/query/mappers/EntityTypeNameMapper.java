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

import hu.blackbelt.judo.meta.query.EntityTypeName;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsEntityTypeName;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class EntityTypeNameMapper<ID> extends RdbmsMapper<EntityTypeName> {

    @Override
    public Stream<? extends RdbmsField> map(final EntityTypeName entityTypeName, RdbmsBuilderContext builderContext) {
        final RdbmsBuilder<?> rdbmsBuilder = builderContext.getRdbmsBuilder();

        return Collections.singleton(RdbmsEntityTypeName.builder()
                .tableName(rdbmsBuilder.getTableName(entityTypeName.getType()))
                .type(entityTypeName.getType())
                .build()).stream();
    }
}
