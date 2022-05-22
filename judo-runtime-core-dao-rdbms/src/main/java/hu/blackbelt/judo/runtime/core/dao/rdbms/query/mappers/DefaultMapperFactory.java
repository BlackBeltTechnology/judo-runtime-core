package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

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
