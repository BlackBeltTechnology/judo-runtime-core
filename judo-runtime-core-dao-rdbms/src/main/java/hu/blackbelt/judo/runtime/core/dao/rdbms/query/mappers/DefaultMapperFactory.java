package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;

import java.util.HashMap;
import java.util.Map;

public class DefaultMapperFactory implements MapperFactory {
    @Override
    public Map<Class, RdbmsMapper> getMappers(RdbmsBuilder rdbmsBuilder) {
        Map<Class, RdbmsMapper> mappers = new HashMap<>();
        mappers.put(Attribute.class, new AttributeMapper(rdbmsBuilder));
        mappers.put(Constant.class, new ConstantMapper(rdbmsBuilder));
        mappers.put(Variable.class, new VariableMapper(rdbmsBuilder));
        mappers.put(IdAttribute.class, new IdAttributeMapper());
        mappers.put(TypeAttribute.class, new TypeAttributeMapper());
        mappers.put(EntityTypeName.class, new EntityTypeNameMapper(rdbmsBuilder));
        mappers.put(SubSelect.class, new SubSelectMapper(rdbmsBuilder));
        mappers.put(SubSelectFeature.class, new SubSelectFeatureMapper());
        return mappers;
    }
}
