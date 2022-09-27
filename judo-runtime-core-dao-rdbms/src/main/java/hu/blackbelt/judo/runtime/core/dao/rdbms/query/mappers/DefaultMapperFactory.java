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

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;

import java.util.HashMap;
import java.util.Map;

public class DefaultMapperFactory<ID> implements MapperFactory<ID> {
	@Override
    public Map<Class<?>, RdbmsMapper<?>> getMappers(RdbmsBuilder<ID> rdbmsBuilder) {
		Map<Class<?>, RdbmsMapper<?>> mappers = new HashMap<>();
        mappers.put(Attribute.class, new AttributeMapper<ID>(rdbmsBuilder));
        mappers.put(Constant.class, new ConstantMapper<ID>(rdbmsBuilder));
        mappers.put(Variable.class, new VariableMapper<ID>(rdbmsBuilder));
        mappers.put(IdAttribute.class, new IdAttributeMapper());
        mappers.put(TypeAttribute.class, new TypeAttributeMapper());
        mappers.put(EntityTypeName.class, new EntityTypeNameMapper<ID>(rdbmsBuilder));
        mappers.put(SubSelect.class, new SubSelectMapper<ID>(rdbmsBuilder));
        mappers.put(SubSelectFeature.class, new SubSelectFeatureMapper());
        return mappers;
    }
}
